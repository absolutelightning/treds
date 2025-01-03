package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const DInsert = "DINSERT"

func RegisterDInsertCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DInsert,
		Validate: validateDInsertCommand(),
		Execute:  executeDInsertCommand(),
		IsWrite:  true,
	})
}

func validateDInsertCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("expected minimum 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeDInsertCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.DInsert(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeBulkString(res)
	}
}
