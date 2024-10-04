package commands

import (
	"fmt"
	"strconv"

	"treds/store"
)

const LRemCommand = "LREM"

func RegisterLRemCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LRemCommand,
		Validate: validateLRemCommand(),
		Execute:  executeLRemCommand(),
		IsWrite:  true,
	})
}

func validateLRemCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeLRemCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		index, err := strconv.Atoi(args[1])
		if err != nil {
			return err.Error()
		}
		err = store.LRem(key, index)
		if err != nil {
			return err.Error()
		}
		return "OK\n"
	}
}
