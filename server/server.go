package server

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	wal "github.com/hashicorp/raft-wal"
	"treds/commands"
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

	tredsCommandRegistry commands.CommandRegistry
	clientTransaction    map[string][]string

	*gnet.BuiltinEventEngine
	fsm              *TredsFsm
	raft             *raft.Raft
	id               raft.ServerID
	raftApplyTimeout time.Duration
}

func New(port, segmentSize int, bindAddr, advertiseAddr, serverId string, applyTimeout time.Duration, servers []BootStrapServer) (*Server, error) {

	commandRegistry := commands.NewRegistry()
	commands.RegisterCommands(commandRegistry)
	tredsStore := store.NewTredsStore()

	//TODO: Default config is good enough for now, but probably need to be tweaked
	config := raft.DefaultConfig()

	serverIdFileName := "server-id"

	if serverId == "" {
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
		Port:                 port,
		tredsCommandRegistry: commandRegistry,
		fsm:                  fsm,
		raft:                 r,
		id:                   config.LocalID,
		raftApplyTimeout:     applyTimeout,
		clientTransaction:    make(map[string][]string),
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

func (ts *Server) OnTraffic(c gnet.Conn) gnet.Action {

	data, _ := c.Next(-1)
	inp := string(data)
	if inp == "" {
		err := fmt.Errorf("empty command")
		respErr := fmt.Sprintf("Error Executing command - %v\n", err.Error())
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(respErr), respErr)))
		if errConn != nil {
			fmt.Println("Error occurred writing to connection", errConn)
		}
		return gnet.None
	}

	// Server Commands

	if strings.ToUpper(inp) == Snapshot {

		if _, ok := ts.clientTransaction[c.RemoteAddr().String()]; ok {
			respondErr(c, fmt.Errorf("please run this command outside transaction"))
			return gnet.None
		}

		// Only writes need to be forwarded to leader
		forwarded, rspFwd, err := ts.forwardRequest(data)
		if err != nil {
			respondErr(c, err)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(rspFwd), rspFwd)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		future := ts.raft.Snapshot()
		if future.Error() != nil {
			respondErr(c, future.Error())
			return gnet.None
		}
		res := "OK"
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
		if errConn != nil {
			respondErr(c, errConn)
		}
		return gnet.None
	}

	if strings.ToUpper(strings.Split(inp, " ")[0]) == Restore {

		if _, ok := ts.clientTransaction[c.RemoteAddr().String()]; ok {
			respondErr(c, fmt.Errorf("please run this command outside transaction"))
			return gnet.None
		}

		// Only writes need to be forwarded to leader
		forwarded, rspFwd, err := ts.forwardRequest(data)
		if err != nil {
			respondErr(c, err)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(rspFwd), rspFwd)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		snapshotPath := strings.Split(inp, " ")[1]

		metaFile := filepath.Join(snapshotPath, "meta.json")

		// Read the file contents
		metaData, err := os.ReadFile(metaFile)
		if err != nil {
			fmt.Println("Error reading file:", err)
			respondErr(c, err)
			return gnet.None
		}

		// Unmarshal the JSON into the SnapshotMeta struct
		var metaSnapshot *raft.SnapshotMeta
		err = json.Unmarshal(metaData, &metaSnapshot)
		if err != nil {
			fmt.Println("Error unmarshaling JSON:", err)
			respondErr(c, err)
			return gnet.None
		}

		file, err := os.Open(filepath.Join(snapshotPath, "state.bin"))
		if err != nil {
			fmt.Println("Error opening file:", err)
			respondErr(c, err)
			return gnet.None
		}
		// Ensure the file is closed when done
		defer file.Close()

		// Since *os.File implements io.Reader, you can directly use it as an io.Reader
		var reader io.Reader = file

		err = ts.raft.Restore(metaSnapshot, reader, 2*time.Minute)
		if err != nil {
			respondErr(c, err)
			return gnet.None
		}
		res := "OK"
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
		if errConn != nil {
			respondErr(c, errConn)
		}
		return gnet.None
	}

	if strings.ToUpper(inp) == Multi {
		// Only writes need to be forwarded to leader
		forwarded, rspFwd, err := ts.forwardRequest(data)
		if err != nil {
			respondErr(c, err)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(rspFwd), rspFwd)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		ts.clientTransaction[c.RemoteAddr().String()] = make([]string, 0)

		res := "OK"
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
		if errConn != nil {
			respondErr(c, errConn)
		}
		return gnet.None
	}

	if strings.ToUpper(inp) == Exec {
		// Only writes need to be forwarded to leader
		forwarded, rspFwd, err := ts.forwardRequest(data)
		if err != nil {
			respondErr(c, err)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(rspFwd), rspFwd)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		if _, ok := ts.clientTransaction[c.RemoteAddr().String()]; ok {
			for _, command := range ts.clientTransaction[c.RemoteAddr().String()] {
				ts.executeCommand(command, c)
			}
			delete(ts.clientTransaction, c.RemoteAddr().String())
		}

		res := "OK"
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
		if errConn != nil {
			respondErr(c, errConn)
		}
		return gnet.None
	}

	if strings.ToUpper(inp) == Discard {
		// Only writes need to be forwarded to leader
		forwarded, rspFwd, err := ts.forwardRequest(data)
		if err != nil {
			respondErr(c, err)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(rspFwd), rspFwd)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		delete(ts.clientTransaction, c.RemoteAddr().String())

		res := "OK"
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
		if errConn != nil {
			respondErr(c, errConn)
		}
		return gnet.None
	}

	// Store Commands

	// Check for transaction first, if transaction just enqueue the command

	if _, ok := ts.clientTransaction[c.RemoteAddr().String()]; ok {
		ts.clientTransaction[c.RemoteAddr().String()] = append(ts.clientTransaction[c.RemoteAddr().String()], inp)
		res := "OK"
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
		if errConn != nil {
			respondErr(c, errConn)
		}
		return gnet.None
	}

	// No Transaction - Now execute the command
	return ts.executeCommand(inp, c)
}

func (ts *Server) executeCommand(inp string, c gnet.Conn) gnet.Action {
	commandStringParts := parseCommand(inp)
	commandReg, err := ts.tredsCommandRegistry.Retrieve(strings.ToUpper(commandStringParts[0]))
	if err != nil {
		respondErr(c, err)
		return gnet.None
	}
	if commandReg.IsWrite {

		// Only writes need to be forwarded to leader
		forwarded, rspFwd, forwardErr := ts.forwardRequest([]byte(inp))

		if forwardErr != nil {
			fmt.Println("forward error:", forwardErr.Error())
			respondErr(c, err)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(rspFwd), rspFwd)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		// Validation need to be done before raft Apply so an error is returned before persisting
		if err = commandReg.Validate(commandStringParts[1:]); err != nil {
			respondErr(c, err)
			return gnet.None
		}

		future := ts.raft.Apply([]byte(inp), ts.raftApplyTimeout)

		if err := future.Error(); err != nil {
			respondErr(c, err)
			return gnet.None
		}
		rsp := future.Response()

		switch rsp.(type) {
		case error:
			err := rsp.(error)
			respondErr(c, err)
			return gnet.None
		default:
			res := "OK"
			_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
		}
	} else {
		if err = commandReg.Validate(commandStringParts[1:]); err != nil {
			respondErr(c, err)
			return gnet.None
		}
		res := commandReg.Execute(commandStringParts[1:], ts.fsm.tredsStore)
		_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
		if errConn != nil {
			fmt.Println("Error occurred writing to connection", errConn)
		}
	}
	return gnet.None
}

func parseCommand(inp string) []string {
	commandString := strings.TrimSpace(inp)
	commandStringParts := strings.Split(commandString, " ")
	return commandStringParts
}

func respondErr(c gnet.Conn, err error) {
	respErr := fmt.Sprintf("Error Executing command - %v\n", err.Error())
	_, errConn := c.Write([]byte(fmt.Sprintf("%d\n%s", len(respErr), respErr)))
	if errConn != nil {
		fmt.Println("Error occurred writing to connection", errConn)
	}
}

func (ts *Server) OnClose(_ gnet.Conn, _ error) gnet.Action {
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

// Read all data from the server
func readAllData(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)

	length, err := readUntilNewline(reader)
	if err != nil {
		return "", err
	}
	lengthInt, err := strconv.Atoi(length[:len(length)-1])
	if err != nil {
		return "", err
	}

	resp, err := readFixedBytes(reader, lengthInt)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

// Function to read from the connection until a newline character
func readUntilNewline(reader *bufio.Reader) (string, error) {
	// Read until '\n'
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}

func readFixedBytes(reader *bufio.Reader, n int) ([]byte, error) {
	// Create a buffer of size n to hold the data
	buffer := make([]byte, n)

	// Read exactly n bytes into the buffer
	_, err := io.ReadFull(reader, buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
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

func (ts *Server) forwardRequest(data []byte) (bool, string, error) {
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

	conn, err := net.Dial("tcp", tredsAddr)
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
	line, rerr := readAllData(conn)
	if rerr != nil {
		fmt.Println("Error occurred reading from connection", tredsAddr)
		return false, "", nil
	}
	return true, line, nil
}
