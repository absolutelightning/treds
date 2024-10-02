package commands

import (
	"fmt"

	"treds/store"
)

const SDiffCommand = "SDIFF"

func RegisterSDiffCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SDiffCommand,
		Validate: validateSDiffCommand(),
		Execute:  executeSDiffCommand(),
	})
}

func validateSDiffCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("expected minimum 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSDiffCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.SDiff(args)
		if err != nil {
			return err.Error()
		}
		return res
	}
}
