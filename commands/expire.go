package commands

import (
	"fmt"
	"strconv"
	"time"

	"treds/store"
)

const ExpireCommand = "EXPIRE"

func RegisterExpireCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ExpireCommand,
		Validate: validateExpireCommand(),
		Execute:  executeExpireCommand(),
	})
}

func validateExpireCommand() ValidationHook {
	return func(args []string) error {
		if len(args) != 2 {
			_, err := strconv.Atoi(args[1])
			if err != nil {
				return err
			}
			return fmt.Errorf("expected 1 argument, got %d", len(args))
		}
		return nil
	}
}

func executeExpireCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		key := args[0]
		seconds, _ := strconv.Atoi(args[1])
		now := time.Now()
		expiryTime := now.Add(time.Duration(seconds) * time.Second)
		return "OK\n", store.Expire(key, expiryTime)
	}
}
