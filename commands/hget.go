package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const HGetCommand = "HGET"

func RegisterHGetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HGetCommand,
		Validate: validateHGetCommand(),
		Execute:  executeHGetCommand(),
	})
}

func validateHGetCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHGetCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		res, err := store.HGet(key, args[1])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeBulkString(res)
	}
}
