package server

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	wal "github.com/hashicorp/raft-wal"
	"treds/commands"
	"treds/resp"
	"treds/server/connPool"
	"treds/store"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/panjf2000/gnet/v2"
)

const Snapshot = "SNAPSHOT"
const Restore = "RESTORE"
const Multi = "MULTI"
const Exec = "EXEC"
const Discard = "DISCARD"

type BootStrapServer struct {
	ID   string
	Host string
	Port int
}

type Server struct {
	Addr string
	Port int

	tredsCommandRegistry       commands.CommandRegistry
	tredsServerCommandRegistry ServerCommandRegistry
	clientTransaction          map[string][]string
	clientTransactionLock      *sync.Mutex

	*gnet.BuiltinEventEngine
	fsm              *TredsFsm
	raft             *raft.Raft
	id               raft.ServerID
	raftApplyTimeout time.Duration
	connP            *connPool.ConnPool
}

func New(port, segmentSize int, bindAddr, advertiseAddr, serverId string, applyTimeout time.Duration, servers []BootStrapServer) (*Server, error) {

	commandRegistry := commands.NewRegistry()
	serverCommandRegistry := NewRegistry()
	commands.RegisterCommands(commandRegistry)
	RegisterCommands(serverCommandRegistry)
	tredsStore := store.NewTredsStore()

	//TODO: Default config is good enough for now, but probably need to be tweaked
	config := raft.DefaultConfig()

	serverIdFileName := "server-id"

	if serverId == "" {
		// try reading from file
		if _, err := os.Stat(serverIdFileName); err == nil {
			// File exists, read the UUID
			fmt.Println("File found. Reading UUID from file... If boostrap error is seen, try removing 'data' directory, " +
				"after backup, which can be restored using RESTORE command")
			data, readErr := os.ReadFile(serverIdFileName)
			if readErr != nil {
				fmt.Println("Error reading UUID from file:", err)
			}
			// Parse the UUID
			id, parseErr := uuid.Parse(string(data))
			if parseErr != nil {
				fmt.Println("Error parsing UUID:", parseErr)
			}
			fmt.Println("UUID read from file:", id)
			config.LocalID = raft.ServerID(id.String())

		} else if os.IsNotExist(err) {
			// File does not exist, generate a new UUID
			fmt.Println("File not found. Generating a new UUID...")
			id := uuid.New()

			// Write the UUID to the file
			err = os.WriteFile(serverIdFileName, []byte(id.String()), 0644)
			if err != nil {
				fmt.Println("Error writing UUID to file:", err)
			}
			fmt.Println("New UUID generated and written to file:", id)
			config.LocalID = raft.ServerID(id.String())
		} else {
			// Other errors (e.g., permission issues)
			fmt.Println("Error checking file:", err)
			id := serverId
			config.LocalID = raft.ServerID(id)
		}
	} else {
		// try reading from file
		if _, err := os.Stat(serverIdFileName); err == nil {
			// File exists, read the UUID
			fmt.Println("File found. Reading UUID from file...")
			data, readErr := os.ReadFile(serverIdFileName)
			if readErr != nil {
				fmt.Println("Error reading UUID from file:", err)
			}
			// Parse the UUID
			id, parseErr := uuid.Parse(string(data))
			if parseErr != nil {
				fmt.Println("Error parsing UUID:", parseErr)
			}
			if id.String() != serverId {
				return nil, fmt.Errorf("UUID does not match, please fix 'server-id' file")
			}
			fmt.Println("UUID read from file:", id)
			config.LocalID = raft.ServerID(id.String())

		} else if os.IsNotExist(err) {
			// File does not exist, generate a new UUID
			fmt.Println("File not found. Generating a new UUID...")
			id := serverId

			// Write the UUID to the file
			err = os.WriteFile(serverIdFileName, []byte(id), 0644)
			if err != nil {
				fmt.Println("Error writing UUID to file:", err)
			}
			fmt.Println("New UUID generated and written to file:", id)
			config.LocalID = raft.ServerID(id)
		} else {
			// Other errors (e.g., permission issues)
			fmt.Println("Error checking file:", err)
			id := serverId
			config.LocalID = raft.ServerID(id)
		}
	}

	//This is the port used by raft for replication and such
	// We can keep it as a separate port or do multiplexing over TCP
	addr := fmt.Sprintf("%s:%d", bindAddr, 8300)

	transport, err := raft.NewTCPTransport(addr, &net.TCPAddr{IP: net.IP(advertiseAddr), Port: port}, 10, time.Second, os.Stdout)

	//TODO: do not panic
	if err != nil {
		return nil, err
	}

	// Use raft wal as a backend store for raft
	dir := filepath.Join("data", string(config.LocalID))

	err = os.MkdirAll(dir, fs.ModeDir|fs.ModePerm)
	if err != nil {

		return nil, err
	}

	w, err := wal.Open(dir, wal.WithSegmentSize(segmentSize))
	if err != nil {

		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore("data", 3, nil)
	if err != nil {
		return nil, err
	}

	fsm := NewTredsFsm(commandRegistry, tredsStore)
	r, err := raft.NewRaft(config, fsm, w, w, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	bootStrapServers := []raft.Server{{ID: config.LocalID, Address: raft.ServerAddress(addr), Suffrage: raft.Voter}}

	for _, server := range servers {
		bootStrapServers = append(bootStrapServers, raft.Server{
			ID:      raft.ServerID(server.ID),
			Address: raft.ServerAddress(fmt.Sprintf("%s:%d", server.Host, server.Port)),
		})
	}

	cluster := r.BootstrapCluster(raft.Configuration{Servers: bootStrapServers})

	err = cluster.Error()
	if err != nil {
		return nil, err
	}

	return &Server{
		Port:                       port,
		tredsCommandRegistry:       commandRegistry,
		tredsServerCommandRegistry: serverCommandRegistry,
		fsm:                        fsm,
		raft:                       r,
		id:                         config.LocalID,
		raftApplyTimeout:           applyTimeout,
		clientTransaction:          make(map[string][]string),
		clientTransactionLock:      &sync.Mutex{},
		connP:                      connPool.NewConnPool(time.Second * 5),
	}, nil
}

func (ts *Server) OnBoot(_ gnet.Engine) gnet.Action {
	fmt.Println("Server started on", ts.Port)
	go func() {
		for {
			ts.fsm.tredsStore.CleanUpExpiredKeys()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return gnet.None
}

func (ts *Server) isServerCommand(command string) bool {
	_, err := ts.tredsServerCommandRegistry.Retrieve(strings.ToUpper(command))
	if err != nil {
		return false
	}
	return true
}

func (ts *Server) executeServerCommand(command, inp string, c gnet.Conn) gnet.Action {
	serverCommandRegistration, err := ts.tredsServerCommandRegistry.Retrieve(command)
	if err != nil {
		ts.RespondErr(c, err)
		return gnet.None
	}
	return serverCommandRegistration.Execute(inp, ts, c)
}

func (ts *Server) GetCommandRegistry() commands.CommandRegistry {
	return ts.tredsCommandRegistry
}

func (ts *Server) GetClientTransaction() map[string][]string {
	return ts.clientTransaction
}

func (ts *Server) GetRaft() *raft.Raft {
	return ts.raft
}

func (ts *Server) GetRaftApplyTimeout() time.Duration {
	return ts.raftApplyTimeout
}

func (ts *Server) LockClientTransaction() {
	ts.clientTransactionLock.Lock()
}

func (ts *Server) UnlockClientTransaction() {
	ts.clientTransactionLock.Unlock()
}

func (ts *Server) OnTraffic(c gnet.Conn) gnet.Action {

	data, _ := c.Next(-1)
	inp := string(data)
	if inp == "" {
		err := fmt.Errorf("empty command")
		respErr := fmt.Sprintf("Error Executing command - %v\n", err.Error())
		_, errConn := c.Write([]byte(resp.EncodeError(respErr)))
		if errConn != nil {
			fmt.Println("Error occurred writing to connection", errConn)
		}
		return gnet.None
	}

	// Server Commands
	command, _, err := parseCommand(inp)
	if err != nil {
		ts.RespondErr(c, err)
		return gnet.None
	}

	if ts.isServerCommand(command) {
		return ts.executeServerCommand(command, inp, c)
	}

	// Check for transaction first, if transaction just enqueue the command
	if _, ok := ts.clientTransaction[c.RemoteAddr().String()]; ok {
		ts.clientTransactionLock.Lock()
		defer ts.clientTransactionLock.Unlock()
		ts.clientTransaction[c.RemoteAddr().String()] = append(ts.clientTransaction[c.RemoteAddr().String()], inp)
		res := "QUEUED"
		_, errConn := c.Write([]byte(resp.EncodeSimpleString(res)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}

	// No Transaction - Now execute the command
	// Store Commands
	return ts.executeCommand(inp, c)
}

func (ts *Server) executeCommand(inp string, c gnet.Conn) gnet.Action {
	command, args, err := parseCommand(inp)
	if err != nil {
		ts.RespondErr(c, err)
		return gnet.None
	}
	commandReg, err := ts.tredsCommandRegistry.Retrieve(strings.ToUpper(command))
	if err != nil {
		ts.RespondErr(c, err)
		return gnet.None
	}
	if commandReg.IsWrite {

		// Only writes need to be forwarded to leader
		forwarded, rspFwd, forwardErr := ts.ForwardRequest([]byte(inp))

		if forwardErr != nil {
			fmt.Println("forward error:", forwardErr.Error())
			ts.RespondErr(c, forwardErr)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(rspFwd))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		// Validation need to be done before raft Apply so an error is returned before persisting
		if err = commandReg.Validate(args); err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}

		future := ts.raft.Apply([]byte(inp), ts.raftApplyTimeout)

		if err := future.Error(); err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}
		rsp := future.Response()

		switch rsp.(type) {
		case error:
			err := rsp.(error)
			ts.RespondErr(c, err)
			return gnet.None
		default:
			_, errConn := c.Write([]byte(rsp.(string)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
		}
	} else {
		if err = commandReg.Validate(args); err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}
		res := commandReg.Execute(args, ts.fsm.tredsStore)
		_, errConn := c.Write([]byte(res))
		if errConn != nil {
			fmt.Println("Error occurred writing to connection", errConn)
		}
	}
	return gnet.None
}

func (ts *Server) RespondErr(c gnet.Conn, err error) {
	_, errConn := c.Write([]byte(resp.EncodeError(err.Error())))
	if errConn != nil {
		fmt.Println("Error occurred writing to connection", errConn)
	}
}

func (ts *Server) OnClose(_ gnet.Conn, _ error) gnet.Action {
	err := ts.connP.Close()
	if err != nil {
		fmt.Println("Error occurred closing connection", err.Error())
	}
	return gnet.None
}

func (ts *Server) convertRaftToTredsAddress(raftAddr string) (string, error) {
	// Split the Raft address into host and port
	host, _, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("invalid Raft address: %s", raftAddr)
	}

	// Replace Raft port (8300) with Treds port (7997)
	stringPort := strconv.Itoa(ts.Port)
	decodedAddr, err := decodeHexAddress(host)
	if err != nil {
		return "", fmt.Errorf("invalid Raft address: %s", raftAddr)
	}
	return net.JoinHostPort(decodedAddr, stringPort), nil
}

// readAllRESPData reads all RESP data from the connection as a string
func readAllRESPData(conn net.Conn) (string, error) {
	defer conn.Close()

	var result string
	reader := bufio.NewReader(conn)

	for {
		// Read data from the connection
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// End of data
				break
			}
			return "", err
		}

		// Append the line to the result
		result += line

		// Check for the end of RESP data
		// If you expect specific termination (like \r\n or protocol-level signal), add logic here
		if isEndOfRESP(result) {
			break
		}
	}

	return result, nil
}

// isEndOfRESP determines if the accumulated data is complete
// Customize this function based on the RESP structure or data you expect
func isEndOfRESP(data string) bool {
	// Example: Customize this condition based on your protocol needs
	// For now, assume the RESP message ends if the last line ends with \r\n
	return strings.HasSuffix(data, "\r\n")
}
func decodeHexAddress(hexAddr string) (string, error) {
	if strings.HasPrefix(hexAddr, "?") {
		hexAddr = hexAddr[1:] // Strip the `?` prefix
	}
	bytes, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", fmt.Errorf("invalid hex address: %v", err)
	}
	return string(bytes), nil
}

func (ts *Server) ForwardRequest(data []byte) (bool, string, error) {
	// create a new channel based pool with an initial capacity of 5 and maximum
	// capacity of 30. The factory will create 5 initial connections and put it
	// into the pool.

	addr, leaderId := ts.raft.LeaderWithID()

	if ts.id == leaderId {
		return false, "", nil
	}

	tredsAddr, err := ts.convertRaftToTredsAddress(string(addr))

	fmt.Println("Treds Leader Address", tredsAddr)

	if err != nil {
		fmt.Println("Error occurred converting raft to treds address", addr)
		return false, "", err
	}

	conn, err := ts.connP.Dial("tcp", tredsAddr)
	if err != nil {
		fmt.Println("Error occurred connecting to treds server", tredsAddr)
		return false, "", nil
	}
	defer conn.Close()
	_, err = conn.Write(data)
	if err != nil {
		fmt.Println("Error occurred writing to connection", tredsAddr)
		return false, "", nil
	}
	line, rerr := readAllRESPData(conn)
	if rerr != nil {
		fmt.Println("Error occurred reading from connection", tredsAddr)
		return false, "", nil
	}
	return true, line, nil
}
