package commands

import (
	"fmt"
	"strconv"
	"treds/store"
)

const ZAddCommand = "ZADD"

func RegisterZAddCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZAddCommand,
		Validate: validateZAddCommand(),
		Execute:  executeZAddCommand(),
	})
}

func validateZAddCommand() ValidationHook {
	return func(args []string) error {
		if (((len(args)) % 2) != 0) || len(args) < 3 {
			return fmt.Errorf("expected 3 or multiple of 3 arguments, got %d", len(args))
		}
		return nil
	}
}

func executeZAddCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		added, err := store.ZAdd(args)
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(added), nil
	}
}
