package commands

import (
	"math"
	"strconv"
	"treds/store"
)

const ZREVRANGELEXKVS = "ZREVRANGELEXKVS"

func RegisterZRevRangeLexKVSCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZREVRANGELEXKVS,
		Validate: validateZRangeLex(),
		Execute:  executeZRevRangeLexKVS(),
	})
}

func executeZRevRangeLexKVS() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		count := strconv.Itoa(math.MaxInt64)
		if len(args) > 2 {
			count = args[2]
		}
		withScore := true
		if len(args) > 3 {
			withScore, _ = strconv.ParseBool(args[3])
		}
		prefix := ""
		if len(args) > 4 {
			prefix = args[4]
		}
		v, err := store.ZRevRangeByLexKVS(args[0], args[1], prefix, count, withScore)
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
