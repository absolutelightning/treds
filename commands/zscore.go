package commands

import (
	"fmt"

	"treds/store"
)

const ZScoreCommand = "ZSCORE"

func RegisterZScoreCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZScoreCommand,
		Validate: validateZScore(),
		Execute:  executeZScoreCommand(),
	})
}

func validateZScore() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("expected  2 arguments, got %d", len(args))
		}
		return nil
	}
}

func executeZScoreCommand() ExecutionHook {
	return func(args []string, store store.Store) string {
		res, err := store.ZScore(args)
		if err != nil {
			return err.Error()
		}
		return res
	}
}
