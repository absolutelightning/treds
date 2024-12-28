package commands

import (
	"treds/resp"
	"treds/store"
)

const RPushCommand = "RPUSH"

func RegisterRPushCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     RPushCommand,
		Validate: validateLPushCommand(),
		Execute:  executeRPushCommand(),
		IsWrite:  true,
	})
}

func executeRPushCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.RPush(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeSimpleString("OK")
	}
}
