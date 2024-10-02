package commands

import (
	"fmt"
	"strings"

	"treds/store"
)

const SIsMember = "SISMEMBER"

func RegisterSIsMemberCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SIsMember,
		Validate: validateSIsMemberCommand(),
		Execute:  executeSIsMemberCommand(),
	})
}

func validateSIsMemberCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSIsMemberCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		res, err := store.SIsMember(key, strings.Join(args[1:], " "))
		if err != nil {
			return "", err
		}
		if res {
			return "1", nil
		} else {
			return "0", nil
		}
	}
}
