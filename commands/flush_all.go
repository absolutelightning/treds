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
	return func(args []string, store store.Store) (string, error) {
		return "OK\n", store.FlushAll()
	}
}
