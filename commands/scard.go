package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const SCardCommand = "SCARD"

func RegisterSCardCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SCardCommand,
		Validate: validateSCardCommand(),
		Execute:  executeSCardCommand(),
	})
}

func validateSCardCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		return nil
	}
}

func executeSCardCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		size, err := store.SCard(key)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(size)
	}
}
