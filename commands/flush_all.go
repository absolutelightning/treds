package commands

import (
	"treds/resp"
	"treds/store"
)

const FlushAll = "FLUSHALL"

func RegisterFlushAllCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     FlushAll,
		Validate: validateDBSize(),
		Execute:  executeFlushAll(),
		IsWrite:  true,
	})
}

func executeFlushAll() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.FlushAll()
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
