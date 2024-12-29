package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const HValsCommand = "HVALS"

func RegisterHValsCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     HValsCommand,
		Validate: validateHValsCommand(),
		Execute:  executeHValsCommand(),
	})
}

func validateHValsCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeHValsCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		res, err := store.HVals(key)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeStringArray(res)
	}
}
