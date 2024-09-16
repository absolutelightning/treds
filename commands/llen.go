package commands

import (
	"fmt"

	"treds/store"
)

const LLenCommand = "LLEN"

func RegisterLLenCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LLenCommand,
		Validate: validateLLenCommand(),
		Execute:  executeLLenCommand(),
	})
}

func validateLLenCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeLLenCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		return store.LLen(key)
	}
}
