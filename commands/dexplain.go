package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const DExplain = "DEXPLAIN"

func RegisterDExplainCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DExplain,
		Validate: validateDExplainCommand(),
		Execute:  executeDExplainCommand(),
	})
}

func validateDExplainCommand() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected 2 argument, got %d", len(args))
		}

		return nil
	}
}

func executeDExplainCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.DExplain(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeBulkString(res)
	}
}
