package store

import (
	"bytes"
	"fmt"
	"golang.org/x/sync/errgroup"
	"strconv"
	"sync"
	radix_tree "treds/datastructures/radix"
)

const NilResp = "(nil)"

type TredsStore struct {
	tree *radix_tree.Tree
}

func NewTredsStore() *TredsStore {
	return &TredsStore{
		tree: radix_tree.New(),
	}
}

func (rs *TredsStore) Get(k string) (string, error) {
	v, ok := rs.tree.Get([]byte(k))
	if !ok {
		return NilResp, nil
	}
	return v.(string), nil
}

func (rs *TredsStore) MGet(args []string) (string, error) {
	results := make([]string, len(args))
	var g errgroup.Group
	var mu sync.Mutex
	for i, arg := range args {
		index := i
		key := arg
		g.Go(func() error {
			res, err := rs.Get(key)
			if err != nil {
				return err
			}
			mu.Lock()
			results[index] = res
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return "", err
	}
	var response bytes.Buffer
	for _, res := range results {
		response.WriteString(fmt.Sprintf("%v\n", res))
	}
	return response.String(), nil
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

func (rs *TredsStore) PrefixScanKeys(cursor, prefix, count string) (string, error) {
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
		key, _, found := iterator.Next()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 {
			result.WriteString(fmt.Sprintf("%v\n", string(key)))
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

func (rs *TredsStore) Keys(regex string) (string, error) {
	iterator := rs.tree.Root().Iterator()
	iterator.PatternMatch(regex)

	var result bytes.Buffer

	for {
		key, _, found := iterator.Next()
		if !found {
			break
		}
		result.WriteString(fmt.Sprintf("%v\n", string(key)))
	}

	return result.String(), nil
}

func (rs *TredsStore) KVS(regex string) (string, error) {
	iterator := rs.tree.Root().Iterator()
	iterator.PatternMatch(regex)

	var result bytes.Buffer

	for {
		key, value, found := iterator.Next()
		if !found {
			break
		}
		result.WriteString(fmt.Sprintf("%v\n%v\n", string(key), value.(string)))
	}

	return result.String(), nil
}

func (rs *TredsStore) Size() (string, error) {
	return strconv.Itoa(rs.tree.Len()), nil
}
