package server

import (
	"fmt"
	"github.com/tidwall/evio"
	"strings"
)

type Server struct {
	Port  int
	ErrCh chan error
}

func New(port int) *Server {
	return &Server{
		ErrCh: make(chan error),
		Port:  port,
	}
}

func (s *Server) Init() {
	var events evio.Events

	// This should always be One
	events.NumLoops = 1 // Single-threaded

	// Handle new connections
	events.Serving = func(s evio.Server) (action evio.Action) {
		fmt.Printf("Server started on %s\n", s.Addrs[0])
		return
	}

	// Handle data read from clients
	events.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
		// Simple command handling: reply with PONG to PING command
		if strings.ToUpper(string(in)) == "PING\n" {
			out = []byte("PONG\n")
		} else {
			out = []byte("UNKNOWN COMMAND\n")
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
