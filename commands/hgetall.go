package commands

import (
	"fmt"

	"treds/store"
)

const HGetAllCommand = "HGETALL"

func RegisterHGetAllCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HGetAllCommand,
		Validate: validateHGetAllCommand(),
		Execute:  executeHGetAllCommand(),
	})
}

func validateHGetAllCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHGetAllCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		return store.HGetAll(key)
	}
}
