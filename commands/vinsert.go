package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const VInsert = "VINSERT"

func RegisterVInsert(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     VInsert,
		Validate: validateVInsert(),
		Execute:  executeVInsert(),
		IsWrite:  true,
	})
}

func validateVInsert() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}
		return nil
	}
}

func executeVInsert() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.VInsert(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
