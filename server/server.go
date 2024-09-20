package server

import (
	"fmt"
	wal "github.com/hashicorp/raft-wal"
	"io/fs"
	"net"
	"os"
	"strings"
	"time"

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

	r, err := raft.NewRaft(config, NewTredsFsm(commandRegistry, tredsStore), w, w, raft.NewInmemSnapshotStore(), transport)
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
	}, nil
}

func (ts *Server) OnBoot(_ gnet.Engine) gnet.Action {
	fmt.Println("Server started on", ts.Port)
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
		res, err := commandReg.Execute(commandStringParts[1:], ts.tredsStore)
		if err != nil {
			respondErr(c, err)
			return gnet.None
		}
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
