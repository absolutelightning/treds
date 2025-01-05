package commands

import (
	"treds/resp"
	"treds/store"
)

const VCreate = "VCREATE"

func RegisterVCreate(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     VCreate,
		Validate: validateVCreate(),
		Execute:  executeVCreate(),
		IsWrite:  true,
	})
}

func validateVCreate() ValidationHook {
	return func(args []string) error {
		return nil
	}
}

func executeVCreate() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.VCreate(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
