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
const Epsilon = 1.19209e-07

type TredsStore struct {
	tree            *radix_tree.Tree
	sortedMaps      map[string]*treemap.Map
	sortedMapsScore map[string]*radix_tree.Tree
	sortedMapsKeys  map[string]*radix_tree.Tree
}

func NewTredsStore() *TredsStore {
	return &TredsStore{
		tree:            radix_tree.New(),
		sortedMaps:      make(map[string]*treemap.Map),
		sortedMapsScore: make(map[string]*radix_tree.Tree),
		sortedMapsKeys:  make(map[string]*radix_tree.Tree),
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

func (rs *TredsStore) ZAdd(args []string) error {
	tm := treemap.NewWith(utils.Float64Comparator)
	if storedTm, ok := rs.sortedMaps[args[0]]; ok {
		tm = storedTm
	}
	sm := radix_tree.New()
	if storedSm, ok := rs.sortedMapsScore[args[0]]; ok {
		sm = storedSm
	}
	sortedKeyMap, ok := rs.sortedMapsKeys[args[0]]
	if !ok {
		sortedKeyMap = radix_tree.New()
	}
	for itr := 1; itr < len(args); itr += 3 {
		score, err := strconv.ParseFloat(args[itr], 64)
		if err != nil {
			return err
		}
		sm.Insert([]byte(args[itr+1]), score)
		radixTree := radix_tree.New()
		storedRadixTree, found := tm.Get(score)
		if found {
			radixTree = storedRadixTree.(*radix_tree.Tree)
		}
		sortedKeyMap, _, _ = sortedKeyMap.Insert([]byte(args[itr+1]), args[itr+2])
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
	rs.sortedMapsScore[args[0]] = sm
	rs.sortedMapsKeys[args[0]] = sortedKeyMap
	return nil
}

func (rs *TredsStore) ZRem(args []string) error {
	storedTm, ok := rs.sortedMaps[args[0]]
	if !ok {
		return nil
	}
	for itr := 1; itr < len(args); itr += 1 {
		key := []byte(args[itr])
		score, found := rs.sortedMapsScore[args[0]].Get(key)
		if !found {
			continue
		}
		scoreFloat := score.(float64)
		storedRadixTree, found := storedTm.Get(scoreFloat)
		if !found {
			continue
		}
		radixTree := storedRadixTree.(*radix_tree.Tree)
		radixTree, _, _ = radixTree.Delete([]byte(args[itr]))
		if radixTree.Len() == 0 {
			storedTm.Remove(score)
		} else {
			storedTm.Put(score, radixTree)
		}
		_, radixTreeFloor := storedTm.Floor(scoreFloat - Epsilon)
		if radixTreeFloor != nil {
			tree := radixTreeFloor.(*radix_tree.Tree)
			maxLeaf, foundMaxLeaf := tree.Root().MaximumLeaf()
			minLeaf, _ := radixTree.Root().MinimumLeaf()
			if foundMaxLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
		}
		_, radixTreeCeiling := storedTm.Ceiling(scoreFloat + Epsilon)
		if radixTreeCeiling != nil {
			tree := radixTreeCeiling.(*radix_tree.Tree)
			minLeaf, _ := tree.Root().MinimumLeaf()
			maxLeaf, foundMaxLeaf := radixTree.Root().MaximumLeaf()
			if foundMaxLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
		}
	}
	rs.sortedMaps[args[0]] = storedTm
	for _, arg := range args[1:] {
		rs.sortedMapsScore[args[0]].Delete([]byte(arg))
		rs.sortedMapsKeys[args[0]].Delete([]byte(arg))
	}
	return nil
}

func (rs *TredsStore) ZRangeByLexKVS(key, cursor, prefix, count string, withScore bool) (string, error) {
	radixTree, ok := rs.sortedMapsKeys[key]
	if !ok {
		return "", nil
	}
	iterator := radixTree.Root().Iterator()
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
			if withScore {
				keyScore, _ := rs.sortedMapsScore[key].Get(storedKey)
				result.WriteString(fmt.Sprintf("%v\n%v\n%v\n", keyScore, string(storedKey), value.(string)))
			} else {
				result.WriteString(fmt.Sprintf("%v\n%v\n", string(storedKey), value.(string)))
			}
			countInt--
		}
		if countInt == 0 {
			break
		}
		index += 1
	}
	return result.String(), nil
}

func (rs *TredsStore) ZRangeByLexKeys(key, cursor, prefix, count string, withScore bool) (string, error) {
	radixTree, ok := rs.sortedMapsKeys[key]
	if !ok {
		return "", nil
	}
	iterator := radixTree.Root().Iterator()
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
		if withScore {

		}
		if index >= startIndex && countInt > 0 && strings.HasPrefix(string(storedKey), prefix) {
			if withScore {
				keyScore, _ := rs.sortedMapsScore[key].Get(storedKey)
				result.WriteString(fmt.Sprintf("%v\n%v\n", keyScore, string(storedKey)))
			} else {
				result.WriteString(fmt.Sprintf("%v\n", string(storedKey)))
			}
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
	_, radixTree := sortedMap.Ceiling(minFloat)
	if radixTree == nil {
		return "", nil
	}
	minKV, _ := radixTree.(*radix_tree.Tree).Root().MinimumLeaf()
	for minKV != nil {
		if countInt == 0 {
			break
		}
		score, _ := rs.sortedMapsScore[key].Get(minKV.Key())
		scoreFloat := score.(float64)
		if scoreFloat > maxFloat {
			break
		}
		if index >= offsetInt {
			if withScore {
				result.WriteString(fmt.Sprintf("%v\n%v\n%v\n", score, string(minKV.Key()), minKV.Value().(string)))
			} else {
				result.WriteString(fmt.Sprintf("%v\n%v\n", string(minKV.Key()), minKV.Value().(string)))
			}
			countInt--
		}
		index++
		minKV = minKV.GetNextLeaf()
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
	_, radixTree := sortedMap.Ceiling(minFloat)
	if radixTree == nil {
		return "", nil
	}
	minKV, _ := radixTree.(*radix_tree.Tree).Root().MinimumLeaf()
	for minKV != nil {
		if countInt == 0 {
			break
		}
		score, _ := rs.sortedMapsScore[key].Get(minKV.Key())
		scoreFloat := score.(float64)
		if scoreFloat > maxFloat {
			break
		}
		if index >= offsetInt {
			if withScore {
				result.WriteString(fmt.Sprintf("%v\n%v\n", score, string(minKV.Key())))
			} else {
				result.WriteString(fmt.Sprintf("%v\n", string(minKV.Key())))
			}
			countInt--
		}
		index++
		minKV = minKV.GetNextLeaf()
	}
	return result.String(), nil
}

func (rs *TredsStore) ZScore(args []string) (string, error) {
	store, ok := rs.sortedMapsScore[args[0]]
	if ok {
		score, found := store.Get([]byte(args[1]))
		if found {
			if num, ok := score.(float64); ok {
				return strconv.FormatFloat(num, 'f', -1, 64), nil
			}
		}
	}
	return "", nil
}

func (rs *TredsStore) ZCard(key string) (int, error) {
	store, ok := rs.sortedMapsKeys[key]
	if !ok {
		return 0, nil
	}
	return store.Len(), nil
}
