package commands

import (
	"fmt"

	"treds/store"
)

const HSetCommand = "HSET"

func RegisterHSetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HSetCommand,
		Validate: validateHSetCommand(),
		Execute:  executeHSetCommand(),
	})
}

func validateHSetCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 3 {
			return fmt.Errorf("expected minimum 3 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHSetCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		err := store.HSet(key, args[1:])
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
