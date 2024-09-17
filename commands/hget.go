package commands

import (
	"fmt"

	"treds/store"
)

const HGetCommand = "HGET"

func RegisterHGetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HGetCommand,
		Validate: validateHGetCommand(),
		Execute:  executeHGetCommand(),
	})
}

func validateHGetCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 3 {
			return fmt.Errorf("expected 3 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHGetCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		return store.HGet(key, args[1])
	}
}
