package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const MGetCommand = "MGET"

func RegisterMGetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     MGetCommand,
		Validate: validateMGet(),
		Execute:  executeMGet(),
	})
}

func validateMGet() ValidationHook {
	return func(args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("expected atlest 1 argument, got %d", len(args))
		}
		return nil
	}
}

func executeMGet() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.MGet(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeStringArray(res)
	}
}
