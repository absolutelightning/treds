package commands

import (
	"math"
	"strconv"
	"treds/store"
)

const ZRANGELEXKEYS = "ZRANGELEXKEYS"

func RegisterZRangeLexKeysCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZRANGELEXKEYS,
		Validate: validateZRangeLex(),
		Execute:  executeZRangeLexKeys(),
	})
}

func executeZRangeLexKeys() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		count := strconv.Itoa(math.MaxInt64)
		if len(args) > 3 {
			count = args[3]
		}
		prefix := ""
		if len(args) > 2 {
			prefix = args[2]
		}
		v, err := store.ZRangeByLexKeys(args[0], args[1], prefix, count)
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
