package store

type Store interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
	PrefixScan(string, string, string) (string, error)
}
