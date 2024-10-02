package commands

import (
	"treds/store"
)

const FlushAll = "FLUSHALL"

func RegisterFlushAllCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     FlushAll,
		Validate: validateDBSize(),
		Execute:  executeFlushAll(),
	})
}

func executeFlushAll() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.FlushAll()
		if err != nil {
			return err.Error()
		}
		return "OK\n"
	}
}
