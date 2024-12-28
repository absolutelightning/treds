package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const SAddCommand = "SADD"

func RegisterSAddCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SAddCommand,
		Validate: validateSAddCommand(),
		Execute:  executeSAddCommand(),
		IsWrite:  true,
	})
}

func validateSAddCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeSAddCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		err := store.SAdd(key, args[1:])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
