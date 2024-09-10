package commands

import (
	"fmt"
	"treds/store"
)

const DeleteCommand = "DEL"

func RegisterDeleteCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DeleteCommand,
		Validate: validateDel(),
		Execute:  executeDel(),
	})
}

func validateDel() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeDel() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		err := store.Delete(args[0])
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
