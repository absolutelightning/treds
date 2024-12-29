package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const HLenCommand = "HLEN"

func RegisterHLenCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HLenCommand,
		Validate: validateHLenCommand(),
		Execute:  executeHLenCommand(),
	})
}

func validateHLenCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHLenCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		size, err := store.HLen(key)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(size)
	}
}
