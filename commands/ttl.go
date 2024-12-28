package commands

import (
	"fmt"
	"strconv"

	"treds/resp"
	"treds/store"
)

const TTLCommand = "TTL"

func RegisterTtlCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     TTLCommand,
		Validate: validateTtlCommand(),
		Execute:  executeTtlCommand(),
	})
}

func validateTtlCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		return nil
	}
}

func executeTtlCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		ttl := store.Ttl(key)
		return resp.EncodeBulkString(strconv.Itoa(ttl))
	}
}
