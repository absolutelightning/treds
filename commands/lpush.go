package commands

import (
	"fmt"

	"treds/store"
)

const LPushCommand = "LPUSH"

func RegisterLPushCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LPushCommand,
		Validate: validateLPushCommand(),
		Execute:  executeLPushCommand(),
	})
}

func validateLPushCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeLPushCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		err := store.LPush(args)
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
