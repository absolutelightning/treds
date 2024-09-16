package store

type Store interface {
	Get(string) (string, error)
	MGet([]string) (string, error)
	MSet([]string) error
	Set(string, string) error
	Delete(string) error
	PrefixScan(string, string, string) (string, error)
	PrefixScanKeys(string, string, string) (string, error)
	DeletePrefix(string) error
	Keys(string) (string, error)
	KVS(string) (string, error)
	Size() (string, error)
	ZAdd([]string) error
	ZRem([]string) error
	ZCard(string) (int, error)
	ZScore([]string) (string, error)
	ZRangeByLexKVS(string, string, string, string, bool) (string, error)
	ZRangeByLexKeys(string, string, string, string, bool) (string, error)
	ZRangeByScoreKeys(string, string, string, string, string, bool) (string, error)
	ZRangeByScoreKVS(string, string, string, string, string, bool) (string, error)
	ZRevRangeByLexKVS(string, string, string, string, bool) (string, error)
	ZRevRangeByLexKeys(string, string, string, string, bool) (string, error)
	ZRevRangeByScoreKeys(string, string, string, string, string, bool) (string, error)
	ZRevRangeByScoreKVS(string, string, string, string, string, bool) (string, error)
	FlushAll() error
	LPush([]string) error
	RPush([]string) error
	LPop(string, int) (string, error)
	RPop(string, int) (string, error)
	LRem(string, int) error
	LSet(string, int, string) error
	LRange(string, int, int) (string, error)
	LLen(string) (string, error)
	LIndex([]string) (string, error)
}
