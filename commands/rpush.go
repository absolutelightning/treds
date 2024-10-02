package commands

import (
	"treds/store"
)

const RPushCommand = "RPUSH"

func RegisterRPushCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     RPushCommand,
		Validate: validateLPushCommand(),
		Execute:  executeRPushCommand(),
	})
}

func executeRPushCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		err := store.RPush(args)
		if err != nil {
			return err.Error()
		}
		return "OK\n"
	}
}
