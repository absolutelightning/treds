package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const DeleteCommand = "DEL"

func RegisterDeleteCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DeleteCommand,
		Validate: validateDel(),
		Execute:  executeDel(),
		IsWrite:  true,
	})
}

func validateDel() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeDel() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.Delete(args[0])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
