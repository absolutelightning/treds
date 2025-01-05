package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const DCreateCollection = "DCREATE"

func RegisterDCreateCollection(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DCreateCollection,
		Validate: validateDCreateCollection(),
		Execute:  executeDCreateCollection(),
		IsWrite:  true,
	})
}

func validateDCreateCollection() ValidationHook {
	return func(args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("expected minimum 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeDCreateCollection() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.DCreateCollection(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
