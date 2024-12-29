package commands

import (
	"fmt"
	"strings"

	"treds/resp"
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
	return func(args []string, store store.Store) string {
		key := args[0]
		res, err := store.SIsMember(key, strings.Join(args[1:], " "))
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		if res {
			return resp.EncodeInteger(1)
		} else {
			return resp.EncodeInteger(0)
		}
	}
}
