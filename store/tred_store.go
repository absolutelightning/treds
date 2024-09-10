package store

import (
	"bytes"
	"fmt"
	tree_map "github.com/absolutelightning/gods/maps/treemap"
	radix_tree "github.com/absolutelightning/radix"
	"strconv"
)

const NilResp = "(nil)"

type TredsStore struct {
	tree    *radix_tree.Tree
	treeMap *tree_map.Map[float64, string]
}

func NewTredsStore() *TredsStore {
	return &TredsStore{
		tree:    radix_tree.New(),
		treeMap: tree_map.New[float64, string](),
	}
}

func (rs *TredsStore) Get(k string) (string, error) {
	v, ok := rs.tree.Get([]byte(k))
	if !ok {
		return NilResp, nil
	}
	return v.(string), nil
}

func (rs *TredsStore) Set(k string, v string) error {
	newTree, _, _ := rs.tree.Insert([]byte(k), v)
	rs.tree = newTree
	return nil
}

func (rs *TredsStore) Delete(k string) error {
	newTree, _, _ := rs.tree.Delete([]byte(k))
	rs.tree = newTree
	return nil
}

func (rs *TredsStore) PrefixScan(cursor, prefix, count string) (string, error) {
	startIndex, err := strconv.Atoi(cursor)
	if err != nil {
		return "", err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return "", err
	}
	iterator := rs.tree.Root().Iterator()
	iterator.SeekPrefix([]byte(prefix))

	index := 0

	var result bytes.Buffer

	for {
		key, value, found := iterator.Next()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 {
			result.WriteString(fmt.Sprintf("%v\n%v\n", string(key), value.(string)))
			countInt--
		}
		if countInt == 0 {
			break
		}
		index += 1
	}
	return result.String(), nil
}

func (rs *TredsStore) DeletePrefix(prefix string) error {
	newTree, _ := rs.tree.DeletePrefix([]byte(prefix))
	rs.tree = newTree
	return nil
}
