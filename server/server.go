package server

import (
	"fmt"
	"strings"
	"treds/commands"
	"treds/store"

	"github.com/panjf2000/gnet/v2"
)

type Server struct {
	Addr string
	Port int

	TredsStore store.Store

	*gnet.BuiltinEventEngine
}

func New(port int) *Server {
	return &Server{
		Port:       port,
		TredsStore: store.NewTredsStore(),
	}
}

func (ts *Server) OnBoot(eng gnet.Engine) gnet.Action {
	fmt.Println("Server started on", ts.Port)
	return gnet.None
}

func (ts *Server) OnTraffic(c gnet.Conn) gnet.Action {
	commandRegistry := commands.NewRegistry()
	commands.RegisterCommands(commandRegistry)

	data, _ := c.Next(-1)
	// Simple command handling: reply with PONG to PING command
	inp := string(data)
	if inp == "" {
		return gnet.None
	}
	if strings.ToUpper(inp) == "PING\n" {
		_, err := c.Write([]byte("PONG\n"))
		if err != nil {
			fmt.Println("Error occurred writing to connection", err)
		}
		return gnet.None
	}
	commandString := strings.TrimSpace(inp)
	commandStringParts := strings.Split(commandString, " ")
	command := strings.ToUpper(commandStringParts[0])
	commandReg, err := commandRegistry.Retrieve(command)
	if err != nil {
		_, err = c.Write([]byte(fmt.Sprintf("Error Executing command - %v\n", err.Error())))
		if err != nil {
			fmt.Println("Error occurred writing to connection", err)
		}
		return gnet.None
	}
	if err = commandReg.Validate(commandStringParts[1:]); err != nil {
		_, err = c.Write([]byte(fmt.Sprintf("Error Executing command - %v\n", err.Error())))
		if err != nil {
			fmt.Println("Error occurred writing to connection", err)
		}
		return gnet.None
	}
	res, err := commandReg.Execute(commandStringParts[1:], ts.TredsStore)
	if err != nil {
		_, err = c.Write([]byte(fmt.Sprintf("Error Executing command - %v\n", err.Error())))
		if err != nil {
			fmt.Println("Error occurred writing to connection", err)
		}
		return gnet.None
	}
	_, err = c.Write([]byte(fmt.Sprintf("%d\n%s", len(res), res)))
	if err != nil {
		fmt.Println("Error occurred writing to connection", err)
	}
	return gnet.None
}

func (ts *Server) OnClose(c gnet.Conn, err error) gnet.Action {
	return gnet.None
}
