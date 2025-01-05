package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const DDropCollection = "DDROP"

func RegisterDDropCollection(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DDropCollection,
		Validate: validateDDropCollection(),
		Execute:  executeDDropCollection(),
		IsWrite:  true,
	})
}

func validateDDropCollection() ValidationHook {
	return func(args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("expected minimum 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeDDropCollection() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.DDropCollection(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
