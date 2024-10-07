package commands

import (
	"fmt"
	"strings"

	"treds/store"
)

type CommandRegistry interface {
	Add(*CommandRegistration) error
	Retrieve(string) (*CommandRegistration, error)
}

type commandRegistry struct {
	commands map[string]*CommandRegistration
}

type ValidationHook func(args []string) error
type ExecutionHook func(args []string, store store.Store) string

type CommandRegistration struct {
	Name     string
	Validate ValidationHook
	Execute  ExecutionHook
	IsWrite  bool
}

func NewRegistry() CommandRegistry {
	return &commandRegistry{
		commands: make(map[string]*CommandRegistration),
	}
}

func (c *commandRegistry) Add(reg *CommandRegistration) error {
	if _, ok := c.commands[reg.Name]; ok {
		return fmt.Errorf("command with name %s already present", reg.Name)
	}

	c.commands[reg.Name] = reg
	return nil
}

func (c *commandRegistry) Retrieve(name string) (*CommandRegistration, error) {
	if _, ok := c.commands[strings.ToUpper(name)]; !ok {
		return nil, fmt.Errorf("command with name %s not found in registry", name)
	}

	return c.commands[strings.ToUpper(name)], nil
}
