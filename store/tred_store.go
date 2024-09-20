package store

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/lists/doublylinkedlist"
	"github.com/emirpasic/gods/maps/hashmap"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/emirpasic/gods/utils"
	"golang.org/x/sync/errgroup"
	radix_tree "treds/datastructures/radix"
)

const NilResp = "(nil)\n"
const Epsilon = 1.19209e-07

type Type int

const (
	KeyValueStore Type = iota
	SortedMapStore
	ListStore
	SetStore
	HashStore
)

type TredsStore struct {
	tree *radix_tree.Tree

	sortedMaps      map[string]*treemap.Map
	sortedMapsScore map[string]map[string]float64
	sortedMapsKeys  map[string]*radix_tree.Tree

	lists map[string]*doublylinkedlist.List

	sets map[string]*hashset.Set

	hashes map[string]*hashmap.Map

	expiry map[string]time.Time
}

func NewTredsStore() *TredsStore {
	return &TredsStore{
		tree:            radix_tree.New(),
		sortedMaps:      make(map[string]*treemap.Map),
		sortedMapsScore: make(map[string]map[string]float64),
		sortedMapsKeys:  make(map[string]*radix_tree.Tree),
		lists:           make(map[string]*doublylinkedlist.List),
		sets:            make(map[string]*hashset.Set),
		hashes:          make(map[string]*hashmap.Map),
		expiry:          make(map[string]time.Time),
	}
}

func (rs *TredsStore) CleanUpExpiredKeys() {
	for key, _ := range rs.expiry {
		if rs.hasExpired(key) {
			_ = rs.Delete(key)
		}
	}
}

func (rs *TredsStore) hasExpired(key string) bool {
	expired := false
	now := time.Now()
	if exp, ok := rs.expiry[key]; ok {
		expired = now.After(exp)
	}
	return expired
}

func (rs *TredsStore) getKeyDetails(key string) Type {
	if rs.hasExpired(key) {
		_ = rs.Delete(key)
		return -1
	}
	return rs.getKeyStore(key)
}

func (rs *TredsStore) getKeyStore(key string) Type {
	_, found := rs.tree.Get([]byte(key))
	if found {
		return KeyValueStore
	}
	if _, ok := rs.sortedMaps[key]; ok {
		return SortedMapStore
	}
	if _, ok := rs.lists[key]; ok {
		return ListStore
	}
	if _, ok := rs.sets[key]; ok {
		return SetStore
	}
	if _, ok := rs.hashes[key]; ok {
		return HashStore
	}
	return -1
}

func (rs *TredsStore) Get(k string) (string, error) {
	storeType := rs.getKeyDetails(k)
	if storeType != KeyValueStore {
		return NilResp, nil
	}
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
	kd := rs.getKeyDetails(k)
	if kd != -1 && kd != KeyValueStore {
		return fmt.Errorf("not key value store")
	}
	rs.tree, _, _ = rs.tree.Insert([]byte(k), v)
	return nil
}

func (rs *TredsStore) Delete(k string) error {
	rs.tree, _, _ = rs.tree.Delete([]byte(k))
	delete(rs.sortedMaps, k)
	delete(rs.sortedMapsScore, k)
	delete(rs.sortedMapsKeys, k)
	delete(rs.lists, k)
	delete(rs.sets, k)
	delete(rs.hashes, k)
	delete(rs.expiry, k)
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
		if rs.hasExpired(string(key)) {
			continue
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
		if rs.hasExpired(string(key)) {
			continue
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
		if rs.hasExpired(string(key)) {
			continue
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
		if rs.hasExpired(string(key)) {
			continue
		}
		result.WriteString(fmt.Sprintf("%v\n%v\n", string(key), value.(string)))
	}

	return result.String(), nil
}

func (rs *TredsStore) Size() (string, error) {
	size := rs.tree.Len() + len(rs.sortedMaps) + len(rs.lists) + len(rs.sets) + len(rs.hashes)
	return strconv.Itoa(size), nil
}

func (rs *TredsStore) ZAdd(args []string) error {
	kd := rs.getKeyDetails(args[0])
	if kd != -1 && kd != SortedMapStore {
		return fmt.Errorf("not sorted map store")
	}
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
	for itr := 1; itr < len(args)-2; itr += 3 {
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
	kd := rs.getKeyDetails(args[0])
	if kd != -1 && kd != SortedMapStore {
		return fmt.Errorf("not sorted map store")
	}
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

func (rs *TredsStore) ZRangeByLexKVS(key, cursor, min, max, count string, withScore bool) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
		if index >= startIndex && countInt > 0 && strings.Compare(string(storedKey), min) >= 0 && strings.Compare(string(storedKey), max) <= 0 {
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

func (rs *TredsStore) ZRangeByLexKeys(key, cursor, min, max, count string, withScore bool) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
		if index >= startIndex && countInt > 0 && strings.Compare(string(storedKey), min) >= 0 && strings.Compare(string(storedKey), max) <= 0 {
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
	kd := rs.getKeyDetails(args[0])
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return 0, fmt.Errorf("not sorted map store")
	}
	store, ok := rs.sortedMapsKeys[key]
	if !ok {
		return 0, nil
	}
	return store.Len(), nil
}

func (rs *TredsStore) ZRevRangeByLexKVS(key, cursor, min, max, count string, withScore bool) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
		if index >= startIndex && countInt > 0 && strings.Compare(string(storedKey), min) >= 0 && strings.Compare(string(storedKey), max) <= 0 {
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

func (rs *TredsStore) ZRevRangeByLexKeys(key, cursor, min, max, count string, withScore bool) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
		if index >= startIndex && countInt > 0 && strings.Compare(string(storedKey), min) >= 0 && strings.Compare(string(storedKey), max) <= 0 {
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
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
	rs.lists = make(map[string]*doublylinkedlist.List)
	rs.sets = make(map[string]*hashset.Set)
	rs.hashes = make(map[string]*hashmap.Map)
	rs.expiry = make(map[string]time.Time)
	return nil
}

func (rs *TredsStore) LPush(args []string) error {
	key := args[0]
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
	if !ok {
		storedList = doublylinkedlist.New()
	}
	for _, arg := range args[1:] {
		storedList.Prepend(arg)
	}
	rs.lists[key] = storedList
	return nil
}

func (rs *TredsStore) RPush(args []string) error {
	key := args[0]
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
	if !ok {
		storedList = doublylinkedlist.New()
	}
	for _, arg := range args[1:] {
		storedList.Append(arg)
	}
	rs.lists[key] = storedList
	return nil
}

func (rs *TredsStore) LIndex(args []string) (string, error) {
	key := args[0]
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return "", fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return "", fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
	if !ok {
		return "0", nil
	}
	return strconv.Itoa(storedList.Size()) + "\n", nil
}

func (rs *TredsStore) LRange(key string, start, stop int) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return "", fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return "", fmt.Errorf("not list store")
	}
	var res strings.Builder
	storedList, ok := rs.lists[key]
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
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return "", fmt.Errorf("not list store")
	}
	var res strings.Builder
	storedList, ok := rs.lists[key]
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

func (rs *TredsStore) SAdd(key string, members []string) error {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return fmt.Errorf("not set store")
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		storedSet = hashset.New()
		rs.sets[key] = storedSet
	}
	for _, member := range members {
		storedSet.Add(member)
	}
	return nil
}

func (rs *TredsStore) SRem(key string, members []string) error {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return fmt.Errorf("not set store")
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		return nil
	}
	for _, member := range members {
		storedSet.Remove(member)
	}
	return nil
}

func (rs *TredsStore) SMembers(key string) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return "", fmt.Errorf("not set store")
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		return "", nil
	}
	var res strings.Builder
	values := storedSet.Values()
	for _, member := range values {
		res.WriteString(member.(string))
		res.WriteString("\n")
	}
	return res.String(), nil
}

func (rs *TredsStore) SIsMember(key string, member string) (bool, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return false, fmt.Errorf("not set store")
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		return false, nil
	}
	return storedSet.Contains(member), nil
}

func (rs *TredsStore) SCard(key string) (int, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return 0, fmt.Errorf("not set store")
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		return 0, nil
	}
	return storedSet.Size(), nil
}

func (rs *TredsStore) SUnion(keys []string) (string, error) {
	for _, key := range keys {
		kd := rs.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return "", fmt.Errorf("not set store")
		}
	}
	unionSet := hashset.New()
	for _, key := range keys {
		storedSet, ok := rs.sets[key]
		if !ok {
			continue
		}
		unionSet = unionSet.Union(storedSet)
	}
	values := unionSet.Values()
	var res strings.Builder
	for _, key := range values {
		res.WriteString(key.(string))
		res.WriteString("\n")
	}
	return res.String(), nil
}

func (rs *TredsStore) SInter(keys []string) (string, error) {
	for _, key := range keys {
		kd := rs.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return "", fmt.Errorf("not set store")
		}
	}
	intersectionSet := hashset.New()
	for _, key := range keys {
		storedSet, ok := rs.sets[key]
		if !ok {
			continue
		}
		intersectionSet = storedSet
		break
	}
	for _, key := range keys {
		storedSet, ok := rs.sets[key]
		if !ok {
			continue
		}
		intersectionSet = intersectionSet.Intersection(storedSet)
	}
	values := intersectionSet.Values()
	var res strings.Builder
	for _, key := range values {
		res.WriteString(key.(string))
		res.WriteString("\n")
	}
	return res.String(), nil
}

func (rs *TredsStore) SDiff(keys []string) (string, error) {
	for _, key := range keys {
		kd := rs.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return "", fmt.Errorf("not set store")
		}
	}
	diffSet, ok := rs.sets[keys[0]]
	if !ok {
		return "", nil
	}
	for _, key := range keys[1:] {
		storedSet, found := rs.sets[key]
		if !found {
			continue
		}
		diffSet = diffSet.Difference(storedSet)
	}
	values := diffSet.Values()
	var res strings.Builder
	for _, key := range values {
		res.WriteString(key.(string))
		res.WriteString("\n")
	}
	return res.String(), nil
}

func (rs *TredsStore) HSet(key string, args []string) error {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		storedMap = hashmap.New()
		rs.hashes[key] = storedMap
	}
	for iter := 0; iter < len(args); iter += 2 {
		storedMap.Put(args[iter], args[iter+1])
	}
	return nil
}

func (rs *TredsStore) HGet(key string, field string) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return "", fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return "", nil
	}
	val, found := storedMap.Get(field)
	if !found {
		return "", nil
	}
	var res strings.Builder
	res.WriteString(val.(string))
	res.WriteString("\n")
	return res.String(), nil
}

func (rs *TredsStore) HGetAll(key string) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return "", fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return "", nil
	}
	var res strings.Builder
	for _, field := range storedMap.Keys() {
		res.WriteString(field.(string))
		res.WriteString("\n")
		value, _ := storedMap.Get(field)
		res.WriteString(value.(string))
		res.WriteString("\n")
	}
	return res.String(), nil
}
func (rs *TredsStore) HLen(key string) (int, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return 0, fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return 0, nil
	}
	return storedMap.Size(), nil
}

func (rs *TredsStore) HDel(key string, fields []string) error {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return nil
	}
	for _, field := range fields {
		storedMap.Remove(field)
	}
	return nil
}

func (rs *TredsStore) HExists(key string, field string) (bool, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return false, fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return false, nil
	}
	_, found := storedMap.Get(field)
	return found, nil
}

func (rs *TredsStore) HKeys(key string) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return "", fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return "", nil
	}
	fields := storedMap.Keys()
	var res strings.Builder
	for _, field := range fields {
		res.WriteString(field.(string))
		res.WriteString("\n")
	}
	return res.String(), nil
}

func (rs *TredsStore) HVals(key string) (string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return "", fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return "", nil
	}
	fields := storedMap.Values()
	var res strings.Builder
	for _, field := range fields {
		res.WriteString(field.(string))
		res.WriteString("\n")
	}
	return res.String(), nil
}

func (rs *TredsStore) Expire(key string, expiration time.Time) error {
	rs.expiry[key] = expiration
	return nil
}

func (rs *TredsStore) Ttl(key string) int {
	if rs.getKeyStore(key) != -1 {
		if expiryTime, ok := rs.expiry[key]; ok {
			return int(expiryTime.Sub(time.Now()).Seconds())
		}
		return -1
	}
	return -2
}
