package store

import (
	radix_tree "treds/datastructures/radix-tree"
)

type radixStore struct {
	tree *radix_tree.Tree
}

// A simple store that is based out of golang maps
func NewRadixStore() Store {
	return &radixStore{
		tree: radix_tree.New(),
	}
}

func (rs *radixStore) Get(k string) (string, error) {
	v, ok := rs.tree.Get([]byte(k))
	if !ok {
		return "(nil)", nil
	}
	return v.(string), nil
}

func (rs *radixStore) Set(k string, v string) error {
	newTree, _, _ := rs.tree.Insert([]byte(k), v)
	rs.tree = newTree
	return nil
}

func (rs *radixStore) Delete(k string) error {
	newTree, _, _ := rs.tree.Delete([]byte(k))
	rs.tree = newTree
	return nil
}
