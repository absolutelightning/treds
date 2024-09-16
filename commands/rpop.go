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
	})
}

func executeRPopCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		count, err := strconv.Atoi(args[1])
		if err != nil {
			return "", err
		}
		res, err := store.RPop(key, count)
		if err != nil {
			return "", err
		}
		return res, nil
	}
}
