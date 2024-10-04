package server

import (
	"bufio"
	"fmt"
	"io/fs"
	"net"
	"os"
	"strings"
	"time"

	wal "github.com/hashicorp/raft-wal"

	"treds/commands"
	"treds/store"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/panjf2000/gnet/v2"
)

type Server struct {
	Addr string
	Port int

	tredsStore           store.Store
	tredsCommandRegistry commands.CommandRegistry

	*gnet.BuiltinEventEngine
	raft *raft.Raft
	id   string
}

func New(port int) (*Server, error) {
	commandRegistry := commands.NewRegistry()
	commands.RegisterCommands(commandRegistry)
	tredsStore := store.NewTredsStore()

	//TODO: Default config is good enough for now, but probably need to be tweaked
	config := raft.DefaultConfig()

	//TODO: server id need to be persisted locally on disk in each node
	// so on restart we keep the same ID, this is important for raft operations
	id := uuid.New()
	config.LocalID = raft.ServerID(id.String())

	//This is the port used by raft for replication and such
	// We can keep it as a separate port or do multiplexing over TCP
	addr := fmt.Sprintf("localhost:%d", 8300)

	//TODO: add config for addr and port
	transport, err := raft.NewTCPTransport(addr, &net.TCPAddr{IP: net.IP("localhost"), Port: port}, 10, time.Second, os.Stdout)

	//TODO: do not panic
	if err != nil {
		return nil, err
	}

	// Use raft wal as a backend store for raft
	dir := fmt.Sprintf("/tmp/%s", id.String())

	err = os.MkdirAll(dir, fs.ModeDir|fs.ModePerm)
	if err != nil {

		return nil, err
	}

	//TODO: make segment as a configuration, or chose the right size.
	w, err := wal.Open(dir, wal.WithSegmentSize(200))
	if err != nil {

		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore("data", 3, nil)
	if err != nil {
		return nil, err
	}

	r, err := raft.NewRaft(config, NewTredsFsm(commandRegistry, tredsStore), w, w, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	//TODO: For now bootstrapping is done for a single node, but we need to either add some command to add nodes to a cluster and then bootstrap it
	// or make the nodes as part of a config
	cluster := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: config.LocalID, Address: raft.ServerAddress(addr), Suffrage: raft.Voter}}})

	err = cluster.Error()
	if err != nil {
		return nil, err
	}

	return &Server{
		Port:                 port,
		tredsStore:           tredsStore,
		tredsCommandRegistry: commandRegistry,
		raft:                 r,
		id:                   id.String(),
	}, nil
}

func (ts *Server) OnBoot(_ gnet.Engine) gnet.Action {
	fmt.Println("Server started on", ts.Port)
	go func() {
		for {
			ts.tredsStore.CleanUpExpiredKeys()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	go func() {
		for {
			time.Sleep(60 * time.Second)
			ts.raft.Snapshot()
		}
	}()
	return gnet.None
}

func (ts *Server) OnTraffic(c gnet.Conn) gnet.Action {

	data, _ := c.Next(-1)
	inp := string(data)
	if inp == "" {
		err := fmt.Errorf("empty command")
		_, errConn := c.Write([]byte(fmt.Sprintf("Error Executing command - %v\n", err.Error())))
		if errConn != nil {
			fmt.Println("Error occurred writing to connection", errConn)
		}
		return gnet.None
	}
	commandStringParts := parseCommand(inp)
	commandReg, err := ts.tredsCommandRegistry.Retrieve(strings.ToUpper(commandStringParts[0]))
	if err != nil {
		respondErr(c, err)
		return gnet.None
	}
	//TODO: Make writes only happen on leader, we can have 2 possible strategies here:
	// - forward to leader
	// - return a special error to the client/sdk and they will retry on the new leader
	if commandReg.IsWrite {

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

		// Validation need to be done before raft Apply so an error is returned before persisting
		if err = commandReg.Validate(commandStringParts[1:]); err != nil {
			respondErr(c, err)
			return gnet.None
		}

		//TODO: make timeout configurable
		// For now we are passing the raw data to raft and then parsing it again in the fsm
		// We could probably parse once in here and pass a serialized data struct to raft,
		// to avoid re-parsing in the fsm
		future := ts.raft.Apply(data, 1*time.Second)

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
		res := commandReg.Execute(commandStringParts[1:], ts.tredsStore)
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
	_, errConn := c.Write([]byte(fmt.Sprintf("Error Executing command - %v\n", err.Error())))
	if errConn != nil {
		fmt.Println("Error occurred writing to connection", errConn)
	}
}

func (ts *Server) OnClose(_ gnet.Conn, _ error) gnet.Action {
	return gnet.None
}

func (ts *Server) forwardRequest(data []byte) (bool, string, error) {
	addr, id := ts.raft.LeaderWithID()
	if string(id) == ts.id {
		return false, "", nil
	}

	//TODO: Add connection pooling to avoid opening a connection per request to the server
	conn, err := net.Dial("tcp", string(addr))
	if err != nil {
		return false, "", nil
	}
	defer conn.Close()
	_, err = conn.Write([]byte(data))
	if err != nil {
		return false, "", nil
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false, "", err
	}
	return true, line, nil
}
