package store

import (
	"bytes"
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"sync"
	radix_tree "treds/datastructures/radix"
)

const NilResp = "(nil)"
const Epsilon = 1e-9

type TredsStore struct {
	tree       *radix_tree.Tree
	sortedMaps map[string]*treemap.Map
}

func NewTredsStore() *TredsStore {
	return &TredsStore{
		tree:       radix_tree.New(),
		sortedMaps: make(map[string]*treemap.Map),
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
	return strconv.Itoa(rs.tree.Len() + len(rs.sortedMaps)), nil
}

func (rs *TredsStore) ZAdd(args []string) (bool, error) {
	key := []byte(args[0])
	rs.tree.Insert(key, nil)
	tm := treemap.NewWith(utils.Float64Comparator)
	if storedTm, ok := rs.sortedMaps[args[0]]; ok {
		tm = storedTm
	}
	for itr := 1; itr < len(args); itr += 3 {
		score, err := strconv.ParseFloat(args[itr], 64)
		if err != nil {
			return false, err
		}
		radixTree := radix_tree.New()
		storedRadixTree, found := tm.Get(score)
		if found {
			radixTree = storedRadixTree.(*radix_tree.Tree)
		}
		radixTree, _, _ = radixTree.Insert([]byte(args[itr+1]), args[itr+2])
		tm.Put(score, radixTree)
		_, radixTreeFloor := tm.Floor(score - Epsilon)
		if radixTreeFloor != nil {
			tree := radixTreeFloor.(*radix_tree.Tree)
			maxLeaf, foundMaxLeaf := tree.Root().MaximumLeaf()
			minLeaf, foundMinLeaf := radixTree.Root().MinimumLeaf()
			if foundMaxLeaf && foundMinLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
		}
		_, radixTreeCeiling := tm.Ceiling(score + Epsilon)
		if radixTreeCeiling != nil {
			tree := radixTreeCeiling.(*radix_tree.Tree)
			minLeaf, foundMaxLeaf := tree.Root().MinimumLeaf()
			maxLeaf, foundMinLeaf := radixTree.Root().MaximumLeaf()
			if foundMaxLeaf && foundMinLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
		}
	}
	rs.sortedMaps[args[0]] = tm
	return true, nil
}

func (rs *TredsStore) ZRangeByLexKVS(key, cursor, prefix, count string) (string, error) {
	sortedMap := rs.sortedMaps[key]
	if sortedMap == nil {
		return "", nil
	}
	_, minValue := sortedMap.Min()
	radixTreeMin := minValue.(*radix_tree.Tree)
	iterator := radixTreeMin.Root().Iterator()
	startIndex, err := strconv.Atoi(cursor)
	if err != nil {
		return "", err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return "", err
	}
	index := 0
	var result bytes.Buffer
	for {
		storedKey, value, found := iterator.Next()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 && strings.HasPrefix(string(storedKey), prefix) {
			result.WriteString(fmt.Sprintf("%v\n%v\n", string(storedKey), value.(string)))
			countInt--
		}
		if countInt == 0 {
			break
		}
		index += 1
	}
	return result.String(), nil
}

func (rs *TredsStore) ZRangeByLexKeys(key, cursor, prefix, count string) (string, error) {
	sortedMap := rs.sortedMaps[key]
	if sortedMap == nil {
		return "", nil
	}
	_, minValue := sortedMap.Min()
	radixTreeMin := minValue.(*radix_tree.Tree)
	iterator := radixTreeMin.Root().Iterator()
	startIndex, err := strconv.Atoi(cursor)
	if err != nil {
		return "", err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return "", err
	}
	index := 0
	var result bytes.Buffer
	for {
		storedKey, _, found := iterator.Next()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 && strings.HasPrefix(string(storedKey), prefix) {
			result.WriteString(fmt.Sprintf("%v\n", string(storedKey)))
			countInt--
		}
		if countInt == 0 {
			break
		}
		index += 1
	}
	return result.String(), nil
}

func (rs *TredsStore) ZRangeByScoreKVS(key, min, max, offset, count string, withScore bool) (string, error) {
	sortedMap := rs.sortedMaps[key]
	if sortedMap == nil {
		return "", nil
	}
	minFloat, err := strconv.ParseFloat(min, 64)
	if err != nil {
		return "", err
	}
	maxFloat, err := strconv.ParseFloat(max, 64)
	if err != nil {
		return "", err
	}
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return "", err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return "", err
	}
	var result bytes.Buffer
	index := 0
	for {
		score, radixTree := sortedMap.Ceiling(minFloat)
		if radixTree == nil || score == nil {
			break
		}
		scoreFloat := score.(float64)
		if scoreFloat > maxFloat {
			break
		}
		lastKV, _ := radixTree.(*radix_tree.Tree).Root().MaximumLeaf()
		minKV, _ := radixTree.(*radix_tree.Tree).Root().MinimumLeaf()
		for minKV != lastKV {
			if index >= offsetInt && countInt > 0 {
				if withScore {
					result.WriteString(fmt.Sprintf("%v\n%v\n%v\n", score, string(minKV.Key()), minKV.Value().(string)))
				} else {
					result.WriteString(fmt.Sprintf("%v\n%v\n", string(minKV.Key()), minKV.Value().(string)))
				}
				countInt--
			}
			minKV = minKV.GetNextLeaf()
		}
		if countInt > 0 {
			if withScore {
				result.WriteString(fmt.Sprintf("%v\n%v\n%v\n", score, string(lastKV.Key()), lastKV.Value().(string)))
			} else {
				result.WriteString(fmt.Sprintf("%v\n%v\n", string(lastKV.Key()), lastKV.Value().(string)))
			}
			countInt--
		}
		minFloat = scoreFloat + Epsilon
	}
	return result.String(), nil
}

func (rs *TredsStore) ZRangeByScoreKeys(key, min, max, offset, count string, withScore bool) (string, error) {
	sortedMap := rs.sortedMaps[key]
	if sortedMap == nil {
		return "", nil
	}
	minFloat, err := strconv.ParseFloat(min, 64)
	if err != nil {
		return "", err
	}
	maxFloat, err := strconv.ParseFloat(max, 64)
	if err != nil {
		return "", err
	}
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return "", err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return "", err
	}
	var result bytes.Buffer
	index := 0
	for {
		score, radixTree := sortedMap.Ceiling(minFloat)
		if radixTree == nil || score == nil {
			break
		}
		scoreFloat := score.(float64)
		if scoreFloat > maxFloat {
			break
		}
		lastKV, _ := radixTree.(*radix_tree.Tree).Root().MaximumLeaf()
		minKV, _ := radixTree.(*radix_tree.Tree).Root().MinimumLeaf()
		for minKV != lastKV {
			if index >= offsetInt && countInt > 0 {
				if withScore {
					result.WriteString(fmt.Sprintf("%v\n%v\n", score, string(minKV.Key())))
				} else {
					result.WriteString(fmt.Sprintf("%v\n", string(minKV.Key())))
				}
				countInt--
			}
			minKV = minKV.GetNextLeaf()
		}
		if countInt > 0 {
			if withScore {
				result.WriteString(fmt.Sprintf("%v\n%v\n", score, string(lastKV.Key())))
			} else {
				result.WriteString(fmt.Sprintf("%v\n", string(lastKV.Key())))
			}
			countInt--
		}
		minFloat = scoreFloat + Epsilon
	}
	return result.String(), nil
}
