package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const GetCommand = "GET"

func RegisterGetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     GetCommand,
		Validate: validateGet(),
		Execute:  executeGet(),
	})
}

func validateGet() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeGet() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.Get(args[0])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeBulkString(res)
	}
}
