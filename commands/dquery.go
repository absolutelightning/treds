package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const DQuery = "DQUERY"

func RegisterDQueryCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DQuery,
		Validate: validateDQueryCommand(),
		Execute:  executeDQueryCommand(),
	})
}

func validateDQueryCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeDQueryCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.DQuery(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeStringArray(res)
	}
}
