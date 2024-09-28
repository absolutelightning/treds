package commands

import (
	"treds/store"
)

const LongestPrefixCommand = "LONGESTPREFIX"

func RegisterLongestPrefixCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     LongestPrefixCommand,
		Validate: validateDeletePrefix(),
		Execute:  executeLongestPrefixCommand(),
	})
}

func executeLongestPrefixCommand() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		prefix := args[0]
		return store.LongestPrefix(prefix)
	}
}
