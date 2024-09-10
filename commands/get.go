package commands

import (
	"fmt"
	"treds/store"
)

const GetCommand = "GET"

func RegisterGetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     GetCommand,
		Validate: validate(),
		Execute:  execute(),
	})
}

func validate() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func execute() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		v, err := store.Get(args[0])
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
