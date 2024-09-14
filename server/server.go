package server

import (
	"fmt"
	"github.com/tidwall/evio"
	"strconv"
	"strings"
	"treds/commands"
	"treds/store"
)

type Server struct {
	Port       int
	ErrCh      chan error
	TredsStore store.Store
}

func New(port int) *Server {
	return &Server{
		ErrCh:      make(chan error),
		Port:       port,
		TredsStore: store.NewTredsStore(),
	}
}

func (s *Server) Init() {

	commandRegistry := commands.NewRegistry()
	commands.RegisterCommands(commandRegistry)

	var events evio.Events

	// numLoops should always be 1 because datastructures do not support MVCC.
	// This is single threaded operation
	events.NumLoops = 1 // Single-threaded

	// Handle new connections
	events.Serving = func(s evio.Server) (action evio.Action) {
		fmt.Printf("Server started on %s\n", s.Addrs[0])
		return
	}

	setCommand, _ := commandRegistry.Retrieve("SET")

	for i := 0; i <= 10000000; i++ {
		setCommand.Execute([]string{"user:" + strconv.Itoa(i), "value_" + strconv.Itoa(i)}, s.TredsStore)
	}

	setCommand, _ = commandRegistry.Retrieve("zadd")

	args := make([]string, 0)
	args = append(args, "ss")
	for i := 0; i <= 100000; i++ {
		args = append(args, strings.Split(fmt.Sprintf("%v user:%v %v", 0, i, i), " ")...)
	}
	setCommand.Execute(args, s.TredsStore)

	// Handle data read from clients
	events.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
		// Simple command handling: reply with PONG to PING command
		inp := string(in)
		if inp == "" {
			return
		}
		if strings.ToUpper(inp) == "PING\n" {
			out = []byte("PONG\n")
		} else {
			commandString := strings.TrimSpace(inp)
			commandStringParts := strings.Split(commandString, " ")
			command := strings.ToUpper(commandStringParts[0])
			commandReg, err := commandRegistry.Retrieve(command)
			if err != nil {
				out = []byte(fmt.Sprintf("Error Executing command - %v\n", err.Error()))
				return
			}
			if err = commandReg.Validate(commandStringParts[1:]); err != nil {
				out = []byte(fmt.Sprintf("Error Validating command - %v\n", err.Error()))
				return
			}
			res, err := commandReg.Execute(commandStringParts[1:], s.TredsStore)
			if err != nil {
				out = []byte(fmt.Sprintf("Error Executing command - %v\n", err.Error()))
				return
			}
			out = []byte(fmt.Sprintf("%d\n%s", len(res), res))
		}
		return
	}

	// Define the address to listen on
	address := fmt.Sprintf("tcp://0.0.0.0:%d", s.Port)

	// Start the server
	if err := evio.Serve(events, address); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}
