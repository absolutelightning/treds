package store

type Snapshot struct {
	store     *Store
	lastIndex uint64
}

type Restore struct {
	store *Store
}
