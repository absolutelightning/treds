package commands

import (
	"fmt"
	"strconv"

	"treds/store"
)

const LRangeCommand = "LRANGE"

func RegisterLRangeCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LRangeCommand,
		Validate: validateLRangeCommand(),
		Execute:  executeLRangeCommand(),
	})
}

func validateLRangeCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 3 {
			return fmt.Errorf("expected 3 argument, got %d", len(args))
		}

		return nil
	}
}

func executeLRangeCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		start, err := strconv.Atoi(args[1])
		if err != nil {
			return "", err
		}
		stop, err := strconv.Atoi(args[1])
		if err != nil {
			return "", err
		}
		res, err := store.LRange(key, start, stop)
		if err != nil {
			return "", err
		}
		return res, nil
	}
}
