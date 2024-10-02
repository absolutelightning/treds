package commands

import (
	"treds/store"
)

const DBSize = "DBSIZE"

func RegisterDBSizeCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     DBSize,
		Validate: validateDBSize(),
		Execute:  executeDBSize(),
	})
}

func validateDBSize() ValidationHook {
	return func(args []string) error {
		return nil
	}
}

func executeDBSize() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.Size()
		if err != nil {
			return err.Error()
		}
		return res
	}
}
