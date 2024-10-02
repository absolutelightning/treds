package commands

import (
	"fmt"
	"strconv"

	"treds/store"
)

const HExistsCommand = "HEXISTS"

func RegisterHExistsCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HExistsCommand,
		Validate: validateHExistsCommand(),
		Execute:  executeHExistsCommand(),
	})
}

func validateHExistsCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHExistsCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		found, err := store.HExists(key, args[1])
		if err != nil {
			return err.Error()
		}
		return strconv.FormatBool(found)
	}
}
