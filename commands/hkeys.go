package commands

import (
	"fmt"

	"treds/store"
)

const HKeysCommand = "HKEYS"

func RegisterHKeysCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HKeysCommand,
		Validate: validateHKeysCommand(),
		Execute:  executeHKeysCommand(),
	})
}

func validateHKeysCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHKeysCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		return store.HKeys(key)
	}
}
