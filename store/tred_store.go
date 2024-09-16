package store

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"

	"github.com/emirpasic/gods/lists/doublylinkedlist"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"golang.org/x/sync/errgroup"
	radix_tree "treds/datastructures/radix"
)

const NilResp = "(nil)\n"
const Epsilon = 1.19209e-07

type TredsStore struct {
	tree *radix_tree.Tree

	sortedMaps      map[string]*treemap.Map
	sortedMapsScore map[string]map[string]float64
	sortedMapsKeys  map[string]*radix_tree.Tree

	list map[string]*doublylinkedlist.List
}

func NewTredsStore() *TredsStore {
	return &TredsStore{
		tree:            radix_tree.New(),
		sortedMaps:      make(map[string]*treemap.Map),
		sortedMapsScore: make(map[string]map[string]float64),
		sortedMapsKeys:  make(map[string]*radix_tree.Tree),
		list:            make(map[string]*doublylinkedlist.List),
	}
}

func (rs *TredsStore) Get(k string) (string, error) {
	var res strings.Builder
	v, ok := rs.tree.Get([]byte(k))
	if !ok {
		return NilResp, nil
	}
	res.WriteString(fmt.Sprintf("%v\n", v))
	return res.String(), nil
}

func (rs *TredsStore) MSet(kvs []string) error {
	var g errgroup.Group
	var mu sync.Mutex
	for itr := 0; itr < len(kvs); itr += 2 {
		g.Go(func() error {
			mu.Lock()
			err := rs.Set(kvs[itr], kvs[itr+1])
			mu.Unlock()
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
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
	var response strings.Builder
	for _, res := range results {
		response.WriteString(res)
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
	startHash, err := strconv.Atoi(cursor)
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

	var result strings.Builder

	seenHash := false
	if cursor == "0" {
		seenHash = true
	}

	nextCursor := uint32(0)

	for {
		key, value, found := iterator.Next()
		if !found {
			break
		}
		hashKey, herr := hash(string(key))
		if herr != nil {
			return "", herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && countInt > 0 {
			result.WriteString(fmt.Sprintf("%v\n%v\n", string(key), value.(string)))
			nextCursor, herr = hash(string(key))
			if herr != nil {
				return "", herr
			}
			countInt--
		}
		if countInt == 0 {
			break
		}
		index += 1
	}
	if countInt != 0 {
		nextCursor = uint32(0)
	}
	result.WriteString(strconv.Itoa(int(nextCursor)) + "\n")
	return result.String(), nil
}

func hash(s string) (uint32, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func (rs *TredsStore) PrefixScanKeys(cursor, prefix, count string) (string, error) {
	startHash, err := strconv.Atoi(cursor)
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

	var result strings.Builder

	seenHash := false
	if cursor == "0" {
		seenHash = true
	}

	nextCursor := uint32(0)

	for {
		key, _, found := iterator.Next()
		if !found {
			break
		}
		hashKey, herr := hash(string(key))
		if herr != nil {
			return "", herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && countInt > 0 {
			result.WriteString(fmt.Sprintf("%v\n", string(key)))
			nextCursor, herr = hash(string(key))
			if herr != nil {
				return "", herr
			}
			countInt--
		}
		if countInt == 0 {
			break
		}
		index += 1
	}
	if countInt != 0 {
		nextCursor = uint32(0)
	}
	result.WriteString(strconv.Itoa(int(nextCursor)) + "\n")
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

	var result strings.Builder

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

	var result strings.Builder

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
	return strconv.Itoa(rs.tree.Len() + len(rs.sortedMaps) + len(rs.list)), nil
}

func (rs *TredsStore) ZAdd(args []string) error {
	tm := treemap.NewWith(utils.Float64Comparator)
	if storedTm, ok := rs.sortedMaps[args[0]]; ok {
		tm = storedTm
	}
	sm := make(map[string]float64)
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
		sm[args[itr+1]] = score
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
			if foundMaxLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
			if foundMinLeaf {
				minLeaf.SetPrevLeaf(maxLeaf)
			}
		}
		_, radixTreeCeiling := tm.Ceiling(score + Epsilon)
		if radixTreeCeiling != nil {
			tree := radixTreeCeiling.(*radix_tree.Tree)
			minLeaf, foundMaxLeaf := tree.Root().MinimumLeaf()
			maxLeaf, foundMinLeaf := radixTree.Root().MaximumLeaf()
			if foundMaxLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
			if foundMinLeaf {
				minLeaf.SetPrevLeaf(maxLeaf)
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
		score, found := rs.sortedMapsScore[args[0]]
		if !found {
			continue
		}
		scoreFloat := score[string(key)]
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
			minLeaf, foundMinLeaf := radixTree.Root().MinimumLeaf()
			if foundMaxLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
			if foundMinLeaf {
				minLeaf.SetPrevLeaf(maxLeaf)
			}
		}
		_, radixTreeCeiling := storedTm.Ceiling(scoreFloat + Epsilon)
		if radixTreeCeiling != nil {
			tree := radixTreeCeiling.(*radix_tree.Tree)
			minLeaf, foundMinLeaf := tree.Root().MinimumLeaf()
			maxLeaf, foundMaxLeaf := radixTree.Root().MaximumLeaf()
			if foundMaxLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
			if foundMinLeaf {
				minLeaf.SetPrevLeaf(maxLeaf)
			}
		}
	}
	rs.sortedMaps[args[0]] = storedTm
	for _, arg := range args[1:] {
		delete(rs.sortedMapsScore[args[0]], arg)
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
	sortedMapKey := rs.sortedMapsScore[key]
	var result strings.Builder
	for {
		storedKey, value, found := iterator.Next()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 && strings.HasPrefix(string(storedKey), prefix) {
			if withScore {
				// Fetch the score
				keyScore, _ := sortedMapKey[string(storedKey)]

				// Convert the floating-point score to a string
				scoreStr := strconv.FormatFloat(keyScore, 'f', -1, 64) // Convert float to string with full precision

				// Append score, key, and value to the result
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(storedKey))
				result.WriteString("\n")
				result.WriteString(value.(string))
				result.WriteString("\n")
			} else {
				// Append only the key and value to the result
				result.WriteString(string(storedKey))
				result.WriteString("\n")
				result.WriteString(value.(string))
				result.WriteString("\n")
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
	var result strings.Builder
	sortedMapKey := rs.sortedMapsScore[key]
	for {
		storedKey, _, found := iterator.Next()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 && strings.HasPrefix(string(storedKey), prefix) {
			if withScore {
				// Fetch the score
				keyScore, _ := sortedMapKey[string(storedKey)]

				// Convert the floating-point score to a string
				scoreStr := strconv.FormatFloat(keyScore, 'f', -1, 64) // Convert float to string with full precision

				// Append score and key to the result
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(storedKey))
				result.WriteString("\n")
			} else {
				// Append only the key to the result
				result.WriteString(string(storedKey))
				result.WriteString("\n")
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
	var result strings.Builder
	index := 0
	_, radixTree := sortedMap.Ceiling(minFloat)
	if radixTree == nil {
		return "", nil
	}
	minKV, _ := radixTree.(*radix_tree.Tree).Root().MinimumLeaf()
	sortedMapKey := rs.sortedMapsScore[key]
	for minKV != nil {
		if countInt == 0 {
			break
		}
		score, _ := sortedMapKey[string(minKV.Key())]
		if score > maxFloat {
			break
		}
		if index >= offsetInt {
			if withScore {
				// Convert the floating-point score to a string
				scoreStr := strconv.FormatFloat(score, 'f', -1, 64) // Convert float to string with full precision

				// Append score, key, and value to the result
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(minKV.Key()))
				result.WriteString("\n")
				result.WriteString(minKV.Value().(string))
				result.WriteString("\n")
			} else {
				// Append only key and value to the result
				result.WriteString(string(minKV.Key()))
				result.WriteString("\n")
				result.WriteString(minKV.Value().(string))
				result.WriteString("\n")
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
	var result strings.Builder
	index := 0
	_, radixTree := sortedMap.Ceiling(minFloat)
	if radixTree == nil {
		return "", nil
	}
	minKV, _ := radixTree.(*radix_tree.Tree).Root().MinimumLeaf()
	sortedMapKey := rs.sortedMapsScore[key]
	for minKV != nil {
		if countInt == 0 {
			break
		}
		score, _ := sortedMapKey[string(minKV.Key())]
		if score > maxFloat {
			break
		}
		if index >= offsetInt {
			if withScore {
				// Convert the floating-point score to a string
				scoreStr := strconv.FormatFloat(score, 'f', -1, 64) // Convert float to string

				// Append score and key to the result
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(minKV.Key()))
				result.WriteString("\n")
			} else {
				// Append only key to the result
				result.WriteString(string(minKV.Key()))
				result.WriteString("\n")
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
	if !ok {
		return "", nil
	}
	if score, found := store[args[1]]; found {
		return strconv.FormatFloat(score, 'f', -1, 64), nil
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

func (rs *TredsStore) ZRevRangeByLexKVS(key, cursor, prefix, count string, withScore bool) (string, error) {
	radixTree, ok := rs.sortedMapsKeys[key]
	if !ok {
		return "", nil
	}
	iterator := radixTree.Root().ReverseIterator()
	startIndex, err := strconv.Atoi(cursor)
	if err != nil {
		return "", err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return "", err
	}
	index := 0
	var result strings.Builder
	sortedMapKey := rs.sortedMapsScore[key]
	for {
		storedKey, value, found := iterator.Previous()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 && strings.HasPrefix(string(storedKey), prefix) {
			if withScore {
				keyScore, _ := sortedMapKey[string(storedKey)]
				scoreStr := strconv.FormatFloat(keyScore, 'f', -1, 64)
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(storedKey))
				result.WriteString("\n")
				result.WriteString(value.(string))
				result.WriteString("\n")
			} else {
				result.WriteString(string(storedKey))
				result.WriteString("\n")
				result.WriteString(value.(string))
				result.WriteString("\n")
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

func (rs *TredsStore) ZRevRangeByLexKeys(key, cursor, prefix, count string, withScore bool) (string, error) {
	radixTree, ok := rs.sortedMapsKeys[key]
	if !ok {
		return "", nil
	}
	iterator := radixTree.Root().ReverseIterator()
	startIndex, err := strconv.Atoi(cursor)
	if err != nil {
		return "", err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return "", err
	}
	index := 0
	var result strings.Builder
	sortedMapKey := rs.sortedMapsScore[key]
	for {
		storedKey, _, found := iterator.Previous()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 && strings.HasPrefix(string(storedKey), prefix) {
			if withScore {
				// Fetch the score
				keyScore, _ := sortedMapKey[string(storedKey)]
				scoreStr := strconv.FormatFloat(keyScore, 'f', -1, 64) // -1 preserves full precision
				// Append keyScore and storedKey to the result
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(storedKey))
				result.WriteString("\n")
			} else {
				// Append only the storedKey to the result
				result.WriteString(string(storedKey))
				result.WriteString("\n")
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

func (rs *TredsStore) ZRevRangeByScoreKVS(key, min, max, offset, count string, withScore bool) (string, error) {
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
	var result strings.Builder
	index := 0
	_, radixTree := sortedMap.Floor(maxFloat)
	if radixTree == nil {
		return "", nil
	}
	maxKV, _ := radixTree.(*radix_tree.Tree).Root().MaximumLeaf()
	sortedMapKey := rs.sortedMapsScore[key]
	for maxKV != nil {
		if countInt == 0 {
			break
		}
		score, _ := sortedMapKey[string(maxKV.Key())]
		if score < minFloat {
			break
		}
		if index >= offsetInt {
			if withScore {
				// Convert the floating-point score to a string
				scoreStr := strconv.FormatFloat(score, 'f', -1, 64) // Convert float to string
				// Append score, key, and value to the result
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(maxKV.Key()))
				result.WriteString("\n")
				result.WriteString(maxKV.Value().(string))
				result.WriteString("\n")
			} else {
				// Append only key and value to the result
				result.WriteString(string(maxKV.Key()))
				result.WriteString("\n")
				result.WriteString(maxKV.Value().(string))
				result.WriteString("\n")
			}
			countInt--
		}
		index++
		maxKV = maxKV.GetPrevLeaf()
	}
	return result.String(), nil
}

func (rs *TredsStore) ZRevRangeByScoreKeys(key, min, max, offset, count string, withScore bool) (string, error) {
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
	var result strings.Builder
	index := 0
	_, radixTree := sortedMap.Floor(maxFloat)
	if radixTree == nil {
		return "", nil
	}
	maxKV, _ := radixTree.(*radix_tree.Tree).Root().MaximumLeaf()
	sortedMapKey := rs.sortedMapsScore[key]
	for maxKV != nil {
		if countInt == 0 {
			break
		}
		score, _ := sortedMapKey[string(maxKV.Key())]
		if score < minFloat {
			break
		}
		if index >= offsetInt {
			if withScore {
				// Convert the floating-point score to a string
				scoreStr := strconv.FormatFloat(score, 'f', -1, 64) // Convert float to string

				// Append score and key to the result
				result.WriteString(scoreStr)
				result.WriteString("\n")
				result.WriteString(string(maxKV.Key()))
				result.WriteString("\n")
			} else {
				// Append only key to the result
				result.WriteString(string(maxKV.Key()))
				result.WriteString("\n")
			}
			countInt--
		}
		index++
		maxKV = maxKV.GetPrevLeaf()
	}
	return result.String(), nil
}

func (rs *TredsStore) FlushAll() error {
	rs.tree = radix_tree.New()
	rs.sortedMaps = make(map[string]*treemap.Map)
	rs.sortedMapsScore = make(map[string]map[string]float64)
	rs.sortedMapsKeys = make(map[string]*radix_tree.Tree)
	rs.list = make(map[string]*doublylinkedlist.List)
	return nil
}

func (rs *TredsStore) LPush(args []string) error {
	key := args[0]
	storedList, ok := rs.list[key]
	if !ok {
		storedList = doublylinkedlist.New()
	}
	for _, arg := range args[1:] {
		storedList.Prepend(arg)
	}
	rs.list[key] = storedList
	return nil
}

func (rs *TredsStore) RPush(args []string) error {
	key := args[0]
	storedList, ok := rs.list[key]
	if !ok {
		storedList = doublylinkedlist.New()
	}
	for _, arg := range args[1:] {
		storedList.Append(arg)
	}
	rs.list[key] = storedList
	return nil
}

func (rs *TredsStore) LIndex(args []string) (string, error) {
	key := args[0]
	storedList, ok := rs.list[key]
	if !ok {
		return "", nil
	}
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return "", err
	}
	if index < 0 {
		index = storedList.Size() + index
	}
	value, found := storedList.Get(index)
	if !found {
		return "", nil
	}
	var result strings.Builder
	result.WriteString(value.(string))
	result.WriteString("\n")
	return result.String(), nil
}

func (rs *TredsStore) LLen(key string) (string, error) {
	storedList, ok := rs.list[key]
	if !ok {
		return "0", nil
	}
	return strconv.Itoa(storedList.Size()) + "\n", nil
}

func (rs *TredsStore) LRange(key string, start, stop int) (string, error) {
	storedList, ok := rs.list[key]
	if !ok {
		return "", nil
	}
	if start < 0 {
		start = storedList.Size() + start
	}
	if stop < 0 {
		stop = storedList.Size() + stop
	}
	if start > stop {
		return "", nil
	}
	vals := storedList.Values()
	var result strings.Builder
	for i := start; i <= stop; i++ {
		result.WriteString(vals[i].(string))
		result.WriteString("\n")
	}
	return result.String(), nil
}

func (rs *TredsStore) LSet(key string, index int, element string) error {
	storedList, ok := rs.list[key]
	if !ok {
		return nil
	}
	if index < 0 {
		index = storedList.Size() + index
	}
	storedList.Set(index, element)
	return nil
}

func (rs *TredsStore) LRem(key string, index int) error {
	storedList, ok := rs.list[key]
	if !ok {
		return nil
	}
	if index < 0 {
		index = storedList.Size() + index
	}
	storedList.Remove(index)
	return nil
}

func (rs *TredsStore) LPop(key string, count int) (string, error) {
	var res strings.Builder
	storedList, ok := rs.list[key]
	if !ok {
		return "", nil
	}
	for count > 0 {
		elem, found := storedList.Get(0)
		if found {
			storedList.Remove(0)
			res.WriteString(elem.(string))
			res.WriteString("\n")
		} else {
			break
		}
		count--
	}
	return res.String(), nil
}

func (rs *TredsStore) RPop(key string, count int) (string, error) {
	var res strings.Builder
	storedList, ok := rs.list[key]
	if !ok {
		return "", nil
	}
	lastIndex := storedList.Size() - 1
	for count > 0 {
		elem, found := storedList.Get(lastIndex)
		if found {
			storedList.Remove(lastIndex)
			lastIndex = storedList.Size() - 1
			res.WriteString(elem.(string))
			res.WriteString("\n")
		} else {
			break
		}
		count--
	}
	return res.String(), nil
}
