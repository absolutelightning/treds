package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const MSETCommand = "MSET"

func RegisterMSetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     MSETCommand,
		Validate: validateMSet(),
		Execute:  executeMSet(),
		IsWrite:  true,
	})
}

func validateMSet() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}
		return nil
	}
}

func executeMSet() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.MSet(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
