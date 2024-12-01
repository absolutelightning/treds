package commands

import (
	"strconv"

	"treds/store"
)

const RPopCommand = "RPOP"

func RegisterRPopCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     RPopCommand,
		Validate: validateLPopCommand(),
		Execute:  executeRPopCommand(),
		IsWrite:  true,
	})
}

func executeRPopCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		key := args[0]
		count, err := strconv.Atoi(args[1])
		if err != nil {
			return err.Error()
		}
		res, err := store.RPop(key, count)
		if err != nil {
			return err.Error()
		}
		return res
	}
}
