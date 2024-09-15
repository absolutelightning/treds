package commands

import (
	"math"
	"strconv"

	"treds/store"
)

const ZREVRANGELEXKEYS = "ZREVRANGELEXKEYS"

func RegisterZRevRangeLexKeysCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZREVRANGELEXKEYS,
		Validate: validateZRangeLex(),
		Execute:  executeZRevRangeLexKeys(),
	})
}

func executeZRevRangeLexKeys() ExecutionHook {
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
		v, err := store.ZRevRangeByLexKeys(args[0], args[1], prefix, count, withScore)
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
