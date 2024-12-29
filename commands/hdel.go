package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const HDelCommand = "HDEL"

func RegisterHDelCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HDelCommand,
		Validate: validateHDelCommand(),
		Execute:  executeHDelCommand(),
		IsWrite:  true,
	})
}

func validateHDelCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHDelCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		err := store.HDel(key, args[1:])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
