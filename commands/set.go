package commands

import (
	"fmt"

	"treds/store"
)

const SetCommand = "SET"

func RegisterSetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SetCommand,
		Validate: validateSet(),
		Execute:  executeSet(),
	})
}

func validateSet() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSet() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		err := store.Set(args[0], args[1])
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
