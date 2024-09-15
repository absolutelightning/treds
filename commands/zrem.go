package commands

import (
	"fmt"

	"treds/store"
)

const ZRemCommand = "ZREM"

func RegisterZRemCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZRemCommand,
		Validate: validateZRem(),
		Execute:  executeZRemCommand(),
	})
}

func validateZRem() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected 3 or multiple of 2 arguments, got %d", len(args))
		}
		return nil
	}
}

func executeZRemCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		err := store.ZRem(args)
		if err != nil {
			return "", err
		}
		return "OK\n", nil
	}
}
