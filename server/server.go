package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

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

func (ts *Server) OnBoot(_ gnet.Engine) gnet.Action {
	setCommand, _ := ts.tredsCommandRegistry.Retrieve("SET")

	for i := 0; i <= 10000000; i++ {
		setCommand.Execute([]string{"user:" + strconv.Itoa(i), "value_" + strconv.Itoa(i)}, ts.tredsStore)
	}

	setCommand, _ = ts.tredsCommandRegistry.Retrieve("ZADD")

	args := make([]string, 0)
	args = append(args, "ss")
	for i := 0; i <= 10000000; i++ {
		args = append(args, strings.Split(fmt.Sprintf("%v user:%v %v", 0, i, i), " ")...)
	}
	setCommand.Execute(args, ts.tredsStore)

	setCommand, _ = ts.tredsCommandRegistry.Retrieve("ZADD")

	args = make([]string, 0)
	args = append(args, "ssd")
	for i := 0; i <= 10000000; i++ {
		args = append(args, strings.Split(fmt.Sprintf("%v user:%v %v", i, i, i), " ")...)
	}
	setCommand.Execute(args, ts.tredsStore)
	fmt.Println("Server started on", ts.Port)
	go func() {
		for {
			ts.tredsStore.CleanUpExpiredKeys()
			time.Sleep(100 * time.Millisecond)
		}
	}()
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
