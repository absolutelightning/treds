package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const BFAdd = "BFADD"

func RegisterBFAddCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     BFAdd,
		Validate: validateBFAdd(),
		Execute:  executeBFAdd(),
	})
}

func validateBFAdd() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected exactly 2 argument, got %d", len(args))
		}
		return nil
	}
}

func executeBFAdd() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.BFAdd(args[0], args[1])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(res)
	}
}
