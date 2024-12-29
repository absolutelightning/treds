package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const LPushCommand = "LPUSH"

func RegisterLPushCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LPushCommand,
		Validate: validateLPushCommand(),
		Execute:  executeLPushCommand(),
		IsWrite:  true,
	})
}

func validateLPushCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeLPushCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.LPush(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
