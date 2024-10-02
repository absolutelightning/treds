package commands

import (
	"fmt"

	"treds/store"
)

const SUnionCommand = "SUNION"

func RegisterSUnionCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SUnionCommand,
		Validate: validateSUnionCommand(),
		Execute:  executeSUnionCommand(),
	})
}

func validateSUnionCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("expected minimum 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSUnionCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.SUnion(args)
		if err != nil {
			return err.Error()
		}
		return res
	}
}
