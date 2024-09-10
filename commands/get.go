package commands

import (
	"fmt"
	"treds/store"
)

const GetCommand = "GET"

func RegisterGetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     GetCommand,
		Validate: validateGet(),
		Execute:  executeGet(),
	})
}

func validateGet() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeGet() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		return store.Get(args[0])
	}
}
