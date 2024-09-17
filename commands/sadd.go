package commands

import (
	"fmt"

	"treds/store"
)

const SAddCommand = "SADD"

func RegisterSAddCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SAddCommand,
		Validate: validateSAddCommand(),
		Execute:  executeSAddCommand(),
	})
}

func validateSAddCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSAddCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		err := store.SAdd(key, args[1:])
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
