package commands

import (
	"fmt"

	"treds/store"
)

const DeletePrefixCommand = "DELPREFIX"

func RegisterDeletePrefixCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DeletePrefixCommand,
		Validate: validateDeletePrefix(),
		Execute:  executeDeletePrefix(),
	})
}

func validateDeletePrefix() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		return nil
	}
}

func executeDeletePrefix() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		err := store.DeletePrefix(args[0])
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
