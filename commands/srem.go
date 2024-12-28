package commands

import (
	"treds/resp"
	"treds/store"
)

const SRemCommand = "SREM"

func RegisterSRemCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     SRemCommand,
		Validate: validateSAddCommand(),
		Execute:  executeSRemCommand(),
		IsWrite:  true,
	})
}

func executeSRemCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		err := store.SRem(key, args[1:])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
