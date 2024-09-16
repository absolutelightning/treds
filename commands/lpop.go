package commands

import (
	"fmt"
	"strconv"

	"treds/store"
)

const LPopCommand = "LPOP"

func RegisterLPopCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LPopCommand,
		Validate: validateLPopCommand(),
		Execute:  executeLPopCommand(),
	})
}

func validateLPopCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeLPopCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		count, err := strconv.Atoi(args[1])
		if err != nil {
			return "", err
		}
		res, err := store.LPop(key, count)
		if err != nil {
			return "", err
		}
		return res, nil
	}
}
