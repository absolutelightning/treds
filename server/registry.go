package server

import (
	"fmt"
	"strings"

	"github.com/panjf2000/gnet/v2"
)

type ServerCommandRegistry interface {
	Add(*ServerCommandRegistration) error
	Retrieve(string) (*ServerCommandRegistration, error)
}

type serverCommandRegistry struct {
	commands map[string]*ServerCommandRegistration
}

type ExecutionHook func(inp string, server *Server, c gnet.Conn) gnet.Action

type ServerCommandRegistration struct {
	Name    string
	Execute ExecutionHook
}

func NewRegistry() ServerCommandRegistry {
	return &serverCommandRegistry{
		commands: make(map[string]*ServerCommandRegistration),
	}
}

func (c *serverCommandRegistry) Add(reg *ServerCommandRegistration) error {
	if _, ok := c.commands[reg.Name]; ok {
		return fmt.Errorf("command with name %s already present", reg.Name)
	}

	c.commands[reg.Name] = reg
	return nil
}

func (c *serverCommandRegistry) Retrieve(name string) (*ServerCommandRegistration, error) {
	if _, ok := c.commands[strings.ToUpper(name)]; !ok {
		return nil, fmt.Errorf("command with name %s not found in registry", name)
	}

	return c.commands[strings.ToUpper(name)], nil
}
