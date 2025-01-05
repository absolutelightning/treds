package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const VDelete = "VDELETE"

func RegisterVDelete(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     VDelete,
		Validate: validateVDelete(),
		Execute:  executeVDelete(),
		IsWrite:  true,
	})
}

func validateVDelete() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}
		return nil
	}
}

func executeVDelete() ExecutionHook {
	return func(args []string, store store.Store) string {
		deleted, err := store.VDelete(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		if deleted {
			return resp.EncodeSimpleString("OK")
		}
		return resp.EncodeSimpleString("NOT_FOUND")
	}
}
