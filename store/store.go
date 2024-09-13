package store

type Store interface {
	Get(string) (string, error)
	MGet([]string) (string, error)
	Set(string, string) error
	Delete(string) error
	PrefixScan(string, string, string) (string, error)
	PrefixScanKeys(string, string, string) (string, error)
	DeletePrefix(string) error
	Keys(string) (string, error)
	KVS(string) (string, error)
	Size() (string, error)
	ZAdd([]string) (bool, error)
	ZRangeByLexKVS(string, string, string, string) (string, error)
	ZRangeByLexKeys(string, string, string, string) (string, error)
}
