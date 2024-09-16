package commands

import (
	"fmt"

	"treds/store"
)

const LIndexCommand = "LINDEX"

func RegisterLIndexCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LIndexCommand,
		Validate: validateLIndexCommand(),
		Execute:  executeLIndexCommand(),
	})
}

func validateLIndexCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeLIndexCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		return store.LIndex(args)
	}
}
