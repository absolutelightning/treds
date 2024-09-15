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

	tredsStore           store.Store
	tredsCommandRegistry commands.CommandRegistry

	*gnet.BuiltinEventEngine
}

func New(port int) *Server {
	commandRegistry := commands.NewRegistry()
	commands.RegisterCommands(commandRegistry)
	return &Server{
		Port:                 port,
		tredsStore:           store.NewTredsStore(),
		tredsCommandRegistry: commandRegistry,
	}
}

func (ts *Server) OnBoot(eng gnet.Engine) gnet.Action {
	fmt.Println("Server started on", ts.Port)
	return gnet.None
}

func (ts *Server) OnTraffic(c gnet.Conn) gnet.Action {
	data, _ := c.Next(-1)
	inp := string(data)
	if inp == "" {
		return gnet.None
	}
	commandString := strings.TrimSpace(inp)
	commandStringParts := strings.Split(commandString, " ")
	command := strings.ToUpper(commandStringParts[0])
	commandReg, err := ts.tredsCommandRegistry.Retrieve(command)
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
	res, err := commandReg.Execute(commandStringParts[1:], ts.tredsStore)
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
