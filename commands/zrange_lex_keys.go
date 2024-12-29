package commands

import (
	"math"
	"strconv"

	"treds/resp"
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
	return func(args []string, store store.Store) string {
		count := strconv.Itoa(math.MaxInt64)
		if len(args) > 2 {
			count = args[2]
		}
		withScore := true
		if len(args) > 3 {
			withScore, _ = strconv.ParseBool(args[3])
		}
		minKey := ""
		if len(args) > 4 {
			minKey = args[4]
		}
		maxKey := ""
		if len(args) > 5 {
			maxKey = args[5]
		}
		v, err := store.ZRangeByLexKeys(args[0], args[1], minKey, maxKey, count, withScore)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeStringArray(v)
	}
}
