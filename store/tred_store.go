package store

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/lists/doublylinkedlist"
	"github.com/emirpasic/gods/maps/hashmap"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/emirpasic/gods/utils"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
	radix_tree "treds/datastructures/radix"
	kvstore "treds/store/proto"
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
	args := strings.Join(kvs, " ")
	keyValues, err := splitCommandWithQuotes(args)
	if err != nil {
		return err
	}
	var g errgroup.Group
	var mu sync.Mutex
	for itr := 0; itr < len(keyValues); itr += 2 {
		g.Go(func() error {
			mu.Lock()
			validKey := validateKey(keyValues[itr])
			if !validKey {
				return fmt.Errorf("invalid key: %s", keyValues[itr])
			}
			err = rs.Set(keyValues[itr], keyValues[itr+1])
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

func (rs *TredsStore) MGet(args []string) ([]string, error) {
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
		return nil, err
	}
	response := make([]string, 0)
	for _, res := range results {
		response = append(response, res)
	}
	return response, nil
}

func (rs *TredsStore) Set(k string, v string) error {
	kd := rs.getKeyDetails(k)
	if kd != -1 && kd != KeyValueStore {
		return fmt.Errorf("not key value store")
	}
	validKey := validateKey(k)
	if !validKey {
		return fmt.Errorf("invalid key: %s", v)
	}
	parsedArgs, err := splitCommandWithQuotes(v)
	if err != nil {
		return err
	}
	rs.tree, _, _ = rs.tree.Insert([]byte(k), parsedArgs[0])
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

func (rs *TredsStore) PrefixScan(cursor, prefix, count string) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	iterator := rs.tree.Root().Iterator()
	iterator.SeekPrefix([]byte(prefix))

	index := 0

	result := make([]string, 0)

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
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && countInt > 0 {
			result = append(result, string(key))
			result = append(result, value.(string))
			nextCursor, herr = hash(string(key))
			if herr != nil {
				return nil, herr
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
	result = append(result, strconv.Itoa(int(nextCursor)))
	return result, nil
}

func hash(s string) (uint32, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func (rs *TredsStore) PrefixScanKeys(cursor, prefix, count string) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	iterator := rs.tree.Root().Iterator()
	iterator.SeekPrefix([]byte(prefix))

	index := 0

	result := make([]string, 0)

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
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && countInt > 0 {
			result = append(result, string(key))
			nextCursor, herr = hash(string(key))
			if herr != nil {
				return nil, herr
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
	result = append(result, strconv.Itoa(int(nextCursor)))
	return result, nil
}

func (rs *TredsStore) DeletePrefix(prefix string) (int, error) {
	newTree, _, numDel := rs.tree.DeletePrefix([]byte(prefix))
	rs.tree = newTree
	return numDel, nil
}

func (rs *TredsStore) Keys(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	iterator := rs.tree.Root().Iterator()
	rx := regexp.MustCompile(regex)
	iterator.PatternMatch(rx)

	seenHash := false
	if cursor == "0" {
		seenHash = true
	}
	nextCursor := uint32(0)

	result := make([]string, 0)

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
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && count > 0 {
			result = append(result, string(key))
			nextCursor, herr = hash(string(key))
			if herr != nil {
				return nil, herr
			}
			count--
		}
		if count == 0 {
			break
		}
	}
	if count != 0 {
		nextCursor = uint32(0)
	}
	result = append(result, strconv.Itoa(int(nextCursor)))
	return result, nil
}

func (rs *TredsStore) KVS(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	iterator := rs.tree.Root().Iterator()
	rx := regexp.MustCompile(regex)
	iterator.PatternMatch(rx)

	result := make([]string, 0)
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
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && count > 0 {
			result = append(result, string(key))
			result = append(result, value.(string))
			nextCursor, herr = hash(string(key))
			if herr != nil {
				return nil, herr
			}
			count--
		}
		if count == 0 {
			break
		}
	}
	if count != 0 {
		nextCursor = uint32(0)
	}
	result = append(result, strconv.Itoa(int(nextCursor)))
	return result, nil
}

func (rs *TredsStore) Size() (int, error) {
	size := rs.tree.Len() + len(rs.sortedMaps) + len(rs.lists) + len(rs.sets) + len(rs.hashes)
	return size, nil
}

func (rs *TredsStore) ZAdd(args []string) error {
	kd := rs.getKeyDetails(args[0])
	if kd != -1 && kd != SortedMapStore {
		return fmt.Errorf("not sorted map store")
	}
	parsedArgs, pErr := splitCommandWithQuotes(strings.Join(args[1:], " "))
	if pErr != nil {
		return pErr
	}
	validKey := validateKey(args[0])
	if !validKey {
		return fmt.Errorf("invalid key")
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
	for itr := 0; itr < len(parsedArgs)-2; itr += 3 {
		validKey = validateKey(parsedArgs[itr+1])
		if !validKey {
			return fmt.Errorf("invalid key")
		}
	}
	for itr := 0; itr < len(parsedArgs)-2; itr += 3 {
		score, err := strconv.ParseFloat(parsedArgs[itr], 64)
		if err != nil {
			return err
		}
		sm[parsedArgs[itr+1]] = score
		radixTree := radix_tree.New()
		storedRadixTree, found := tm.Get(score)
		if found {
			radixTree = storedRadixTree.(*radix_tree.Tree)
		}
		sortedKeyMap, _, _ = sortedKeyMap.Insert([]byte(parsedArgs[itr+1]), parsedArgs[itr+2])
		radixTree, _, _ = radixTree.Insert([]byte(parsedArgs[itr+1]), parsedArgs[itr+2])
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
			storedTm.Remove(scoreFloat)
		} else {
			storedTm.Put(scoreFloat, radixTree)
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
		rs.sortedMapsKeys[args[0]], _, _ = rs.sortedMapsKeys[args[0]].Delete([]byte(arg))
	}
	return nil
}

func (rs *TredsStore) ZRangeByLexKVS(key, cursor, min, max, count string, withScore bool) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	radixTree, ok := rs.sortedMapsKeys[key]
	if !ok {
		return nil, nil
	}
	iterator := radixTree.Root().Iterator()
	startIndex, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	index := 0
	sortedMapKey := rs.sortedMapsScore[key]
	result := make([]string, 0)
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
				result = append(result, scoreStr)
				result = append(result, string(storedKey))
				result = append(result, value.(string))
			} else {
				// Append only the key and value to the result
				result = append(result, string(storedKey))
				result = append(result, value.(string))
			}
			countInt--
		}
		if countInt == 0 || strings.Compare(string(storedKey), max) > 0 {
			break
		}
		index += 1
	}
	return result, nil
}

func (rs *TredsStore) ZRangeByLexKeys(key, cursor, min, max, count string, withScore bool) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	radixTree, ok := rs.sortedMapsKeys[key]
	if !ok {
		return nil, nil
	}
	iterator := radixTree.Root().Iterator()
	startIndex, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	index := 0
	result := make([]string, 0)
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
				result = append(result, scoreStr)
				result = append(result, string(storedKey))
			} else {
				// Append only the key to the result
				result = append(result, string(storedKey))
			}
			countInt--
		}
		if countInt == 0 || strings.Compare(string(storedKey), max) > 0 {
			break
		}
		index += 1
	}
	return result, nil
}

func (rs *TredsStore) ZRangeByScoreKVS(key, min, max, offset, count string, withScore bool) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	sortedMap := rs.sortedMaps[key]
	if sortedMap == nil {
		return nil, nil
	}
	minFloat, err := strconv.ParseFloat(min, 64)
	if err != nil {
		return nil, err
	}
	maxFloat, err := strconv.ParseFloat(max, 64)
	if err != nil {
		return nil, err
	}
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	index := 0
	_, radixTree := sortedMap.Ceiling(minFloat)
	if radixTree == nil {
		return nil, nil
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
				result = append(result, scoreStr)
				result = append(result, string(minKV.Key()))
				result = append(result, minKV.Value().(string))
			} else {
				// Append only key and value to the result
				result = append(result, string(minKV.Key()))
				result = append(result, minKV.Value().(string))
			}
			countInt--
		}
		index++
		minKV = minKV.GetNextLeaf()
	}
	return result, nil
}

func (rs *TredsStore) ZRangeByScoreKeys(key, min, max, offset, count string, withScore bool) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	sortedMap := rs.sortedMaps[key]
	if sortedMap == nil {
		return nil, nil
	}
	minFloat, err := strconv.ParseFloat(min, 64)
	if err != nil {
		return nil, err
	}
	maxFloat, err := strconv.ParseFloat(max, 64)
	if err != nil {
		return nil, err
	}
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	index := 0
	_, radixTree := sortedMap.Ceiling(minFloat)
	if radixTree == nil {
		return nil, nil
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
				result = append(result, scoreStr)
				result = append(result, string(minKV.Key()))
			} else {
				// Append only key to the result
				result = append(result, string(minKV.Key()))
			}
			countInt--
		}
		index++
		minKV = minKV.GetNextLeaf()
	}
	return result, nil
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
		if countInt == 0 || strings.Compare(string(storedKey), min) < 0 {
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
		if countInt == 0 || strings.Compare(string(storedKey), min) < 0 {
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
	validKey := validateKey(key)
	if !validKey {
		return fmt.Errorf("invalid key")
	}
	storedList, ok := rs.lists[key]
	if !ok {
		storedList = doublylinkedlist.New()
	}
	parsedArgs, err := splitCommandWithQuotes(strings.Join(args[1:], " "))
	if err != nil {
		return err
	}
	for _, arg := range parsedArgs {
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
	validKey := validateKey(key)
	if !validKey {
		return fmt.Errorf("invalid key")
	}
	storedList, ok := rs.lists[key]
	if !ok {
		storedList = doublylinkedlist.New()
	}
	parsedArgs, err := splitCommandWithQuotes(strings.Join(args[1:], " "))
	if err != nil {
		return err
	}
	for _, arg := range parsedArgs {
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
	return value.(string), nil
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
	return strconv.Itoa(storedList.Size()), nil
}

func (rs *TredsStore) LRange(key string, start, stop int) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return nil, fmt.Errorf("not list store")
	}
	storedList, ok := rs.lists[key]
	if !ok {
		return nil, nil
	}
	if start < 0 {
		start = storedList.Size() + start
	}
	if stop < 0 {
		stop = storedList.Size() + stop
	}
	if start > stop {
		return nil, nil
	}
	vals := storedList.Values()
	result := make([]string, 0)
	for i := start; i <= stop; i++ {
		result = append(result, vals[i].(string))
	}
	return result, nil
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

func (rs *TredsStore) LPop(key string, count int) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return nil, fmt.Errorf("not list store")
	}
	res := make([]string, 0)
	storedList, ok := rs.lists[key]
	if !ok {
		return nil, nil
	}
	for count > 0 {
		elem, found := storedList.Get(0)
		if found {
			storedList.Remove(0)
			res = append(res, elem.(string))
		} else {
			break
		}
		count--
	}
	return res, nil
}

func (rs *TredsStore) RPop(key string, count int) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return nil, fmt.Errorf("not list store")
	}
	res := make([]string, 0)
	storedList, ok := rs.lists[key]
	if !ok {
		return nil, nil
	}
	lastIndex := storedList.Size() - 1
	for count > 0 {
		elem, found := storedList.Get(lastIndex)
		if found {
			storedList.Remove(lastIndex)
			lastIndex = storedList.Size() - 1
			res = append(res, elem.(string))
		} else {
			break
		}
		count--
	}
	return res, nil
}

func (rs *TredsStore) SAdd(key string, members []string) error {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return fmt.Errorf("not set store")
	}
	validKey := validateKey(key)
	if !validKey {
		return fmt.Errorf("invalid key")
	}
	parsedArgs, err := splitCommandWithQuotes(strings.Join(members, " "))
	if err != nil {
		return err
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		storedSet = hashset.New()
		rs.sets[key] = storedSet
	}
	for _, member := range parsedArgs {
		storedSet.Add(member)
	}
	return nil
}

func (rs *TredsStore) SRem(key string, members []string) error {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return fmt.Errorf("not set store")
	}
	parsedArgs, err := splitCommandWithQuotes(strings.Join(members, " "))
	if err != nil {
		return err
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		return nil
	}
	for _, member := range parsedArgs {
		storedSet.Remove(member)
	}
	return nil
}

func (rs *TredsStore) SMembers(key string) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return nil, fmt.Errorf("not set store")
	}
	storedSet, ok := rs.sets[key]
	if !ok {
		return nil, nil
	}
	res := make([]string, 0)
	values := storedSet.Values()
	for _, member := range values {
		res = append(res, member.(string))
	}
	return res, nil
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

func (rs *TredsStore) SUnion(keys []string) ([]string, error) {
	for _, key := range keys {
		kd := rs.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return nil, fmt.Errorf("not set store")
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
	res := make([]string, 0)
	for _, key := range values {
		res = append(res, key.(string))
	}
	return res, nil
}

func (rs *TredsStore) SInter(keys []string) ([]string, error) {
	for _, key := range keys {
		kd := rs.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return nil, fmt.Errorf("not set store")
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
	res := make([]string, 0)
	for _, key := range values {
		res = append(res, key.(string))
	}
	return res, nil
}

func (rs *TredsStore) SDiff(keys []string) ([]string, error) {
	for _, key := range keys {
		kd := rs.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return nil, fmt.Errorf("not set store")
		}
	}
	diffSet, ok := rs.sets[keys[0]]
	if !ok {
		return nil, nil
	}
	for _, key := range keys[1:] {
		storedSet, found := rs.sets[key]
		if !found {
			continue
		}
		diffSet = diffSet.Difference(storedSet)
	}
	values := diffSet.Values()
	res := make([]string, 0)
	for _, key := range values {
		res = append(res, key.(string))
	}
	return res, nil
}

func (rs *TredsStore) HSet(key string, args []string) error {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return fmt.Errorf("not hash store")
	}
	validKey := validateKey(key)
	if !validKey {
		return fmt.Errorf("invalid key")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		storedMap = hashmap.New()
		rs.hashes[key] = storedMap
	}
	parsedArgs, err := splitCommandWithQuotes(strings.Join(args, " "))
	if err != nil {
		return err
	}
	for iter := 0; iter < storedMap.Size(); iter += 2 {
		validKey = validateKey(parsedArgs[iter])
		if !validKey {
			return fmt.Errorf("invalid key")
		}
	}
	for iter := 0; iter < len(parsedArgs); iter += 2 {
		storedMap.Put(parsedArgs[iter], parsedArgs[iter+1])
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

func (rs *TredsStore) HGetAll(key string) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return nil, fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return nil, nil
	}
	res := make([]string, 0)
	for _, field := range storedMap.Keys() {
		res = append(res, field.(string))
		value, _ := storedMap.Get(field)
		res = append(res, value.(string))
	}
	return res, nil
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

func (rs *TredsStore) HKeys(key string) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return nil, fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return nil, nil
	}
	fields := storedMap.Keys()
	res := make([]string, 0)
	for _, field := range fields {
		res = append(res, field.(string))
	}
	return res, nil
}

func (rs *TredsStore) HVals(key string) ([]string, error) {
	kd := rs.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return nil, fmt.Errorf("not hash store")
	}
	storedMap, ok := rs.hashes[key]
	if !ok {
		return nil, nil
	}
	fields := storedMap.Values()
	res := make([]string, 0)
	for _, field := range fields {
		res = append(res, field.(string))
	}
	return res, nil
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

func (rs *TredsStore) LongestPrefix(prefix string) ([]string, error) {
	res := make([]string, 0)
	key, val, found := rs.tree.Root().LongestPrefix([]byte(prefix))
	if found {
		res = append(res, string(key))
		res = append(res, val.(string))
		return res, nil
	}
	return nil, nil
}

func convertToString(value interface{}) (string, error) {
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("value is not a string")
	}
	return str, nil
}

func (rs *TredsStore) Snapshot() ([]byte, error) {
	// For now just persisting the root level key value store
	// That is tree *radix_tree.Tree in the Store
	store := &kvstore.KeyValueStore{
		Pairs: make([]*kvstore.KeyValue, 0),
	}
	minLeaf, found := rs.tree.Root().MinimumLeaf()
	if !found {
		return []byte{}, nil
	}
	for minLeaf != nil {
		value := minLeaf.Value()
		valueString, err := convertToString(value)
		if err != nil {
			return nil, err
		}
		store.Pairs = append(store.Pairs, &kvstore.KeyValue{
			Key:   string(minLeaf.Key()),
			Value: valueString,
		})
		minLeaf = minLeaf.GetNextLeaf()
	}
	data, err := proto.Marshal(store)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (rs *TredsStore) Restore(data []byte) error {
	var deserializedStore kvstore.KeyValueStore
	err := proto.Unmarshal(data, &deserializedStore)
	if err != nil {
		fmt.Println("Error deserializing KeyValueStore:", err)
		return err
	}
	// Print the deserialized key-value pairs
	rs.tree = radix_tree.New()
	fmt.Println("Deserialized KeyValueStore:")
	for _, pair := range deserializedStore.Pairs {
		fmt.Printf("Key: %s, Value: %s\n", pair.Key, pair.Value)
		rs.tree, _, _ = rs.tree.Insert([]byte(pair.Key), pair.Value)
	}
	return nil
}
