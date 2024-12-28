package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const SInterCommand = "SINTER"

func RegisterSInterCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SInterCommand,
		Validate: validateSInterCommand(),
		Execute:  executeSInterCommand(),
	})
}

func validateSInterCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("expected minimum 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSInterCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.SInter(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeStringArray(res)
	}
}
