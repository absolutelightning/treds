package commands

import (
	"fmt"

	"treds/store"
)

const SMembersCommand = "SMEMBERS"

func RegisterSMembersCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SMembersCommand,
		Validate: validateSMembersCommand(),
		Execute:  executeSMembersCommand(),
	})
}

func validateSMembersCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSMembersCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		res, err := store.SMembers(key)
		if err != nil {
			return err.Error()
		}
		return res
	}
}
