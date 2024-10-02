package commands

import (
	"fmt"
	"strconv"
	"strings"

	"treds/store"
)

const LSetCommand = "LSET"

func RegisterLSetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LSetCommand,
		Validate: validateLSetCommand(),
		Execute:  executeLSetCommand(),
	})
}

func validateLSetCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 3 {
			return fmt.Errorf("expected 3 argument, got %d", len(args))
		}
		return nil
	}
}

func executeLSetCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		index, err := strconv.Atoi(args[1])
		if err != nil {
			return "", err
		}
		err = store.LSet(key, index, strings.Join(args[2:], " "))
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
