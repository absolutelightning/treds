package commands

import (
	"fmt"
	"treds/store"
)

const ZAddCommand = "ZADD"

func RegisterZAddCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZAddCommand,
		Validate: validateZAddCommand(),
		Execute:  executeZAddCommand(),
	})
}

func validateZAddCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 3 {
			return fmt.Errorf("expected 3 or multiple of 3 arguments, got %d", len(args))
		}
		return nil
	}
}

func executeZAddCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		err := store.ZAdd(args)
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
