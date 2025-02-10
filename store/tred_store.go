package store

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absolutelightning/bloom"
	"github.com/absolutelightning/gods/lists/doublylinkedlist"
	"github.com/absolutelightning/gods/maps/hashmap"
	"github.com/absolutelightning/gods/maps/treemap"
	"github.com/absolutelightning/gods/sets/hashset"
	"github.com/absolutelightning/gods/utils"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
	"treds/datastructures/hnsw"
	radix_tree "treds/datastructures/radix"
	kvstore "treds/store/proto"
)

const NilResp = "(nil)"
const Unique = "unique"
const IndexSuffix = "_index"

type Type int

const (
	KeyValueStore Type = iota
	SortedMapStore
	ListStore
	SetStore
	HashStore
	DocumentStore
	VectorStore
	BloomFilterStore
)

type Query struct {
	Filters []QueryFilter
	Sort    []Sort
	Limit   int
	Offset  int
}

type QueryFilter struct {
	Field      string        // Field name (e.g., "age", "salary")
	Operator   string        // Comparison operator (e.g., "$gt", "$lt", "$eq")
	Value      interface{}   // Value for the operator
	SubFilters []QueryFilter // Nested filters for logical operators
	Logical    string        // Logical operator: "$and", "$or", "$not"
}

type Sort struct {
	Field string // Field to sort by
	Order string // "asc" for ascending, "desc" for descending
}

// TypeMapping maps schema type strings to their Go reflect.Type equivalents
var TypeMapping = map[string]reflect.Type{
	"string": reflect.TypeOf(""),
	"float":  reflect.TypeOf(0.0),
	"bool":   reflect.TypeOf(true),
}

// CompoundKey represents a key with an array of fields
type CompoundKey struct {
	Fields []string // Array of fields for the key
}

type IndexValues struct {
	FieldValues []interface{}
}

type Index struct {
	indexer  *treemap.Map
	Fields   *CompoundKey
	isUnique bool
}

type Document struct {
	Id         string
	StringData string
	Fields     map[string]interface{}
}

type Collection struct {
	Documents       map[string]*Document
	Indices         map[string]*Index
	Schema          map[string]interface{}
	DocumentIdIndex map[string]map[string]struct{}
}

type TredsStore struct {
	// Key Value Store
	tree *radix_tree.Tree

	// Sorted Maps Store
	sortedMaps      map[string]*treemap.Map
	sortedMapsScore map[string]map[string]float64
	sortedMapsKeys  map[string]*radix_tree.Tree

	// List Store
	lists map[string]*doublylinkedlist.List

	// Set Store
	sets map[string]*hashset.Set

	// Hash Store
	hashes map[string]*hashmap.Map

	// Document Store
	collections map[string]*Collection

	// Vector Store
	vectors map[string]*hnsw.HNSW

	// Bloom Filter Store
	bloomFilters map[string]*bloom.BloomFilter

	// Expiry
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
		collections:     make(map[string]*Collection),
		vectors:         make(map[string]*hnsw.HNSW),
	}
}

func (ts *TredsStore) CleanUpExpiredKeys() {
	for key, _ := range ts.expiry {
		if ts.hasExpired(key) {
			_ = ts.Delete(key)
		}
	}
}

func (ts *TredsStore) hasExpired(key string) bool {
	expired := false
	now := time.Now()
	if exp, ok := ts.expiry[key]; ok {
		expired = now.After(exp)
	}
	return expired
}

func (ts *TredsStore) getKeyDetails(key string) Type {
	if ts.hasExpired(key) {
		_ = ts.Delete(key)
		return -1
	}
	return ts.getKeyStore(key)
}

func (ts *TredsStore) getKeyStore(key string) Type {
	_, found := ts.tree.Get([]byte(key))
	if found {
		return KeyValueStore
	}
	if _, ok := ts.sortedMaps[key]; ok {
		return SortedMapStore
	}
	if _, ok := ts.lists[key]; ok {
		return ListStore
	}
	if _, ok := ts.sets[key]; ok {
		return SetStore
	}
	if _, ok := ts.hashes[key]; ok {
		return HashStore
	}
	return -1
}

func (ts *TredsStore) Get(k string) (string, error) {
	storeType := ts.getKeyDetails(k)
	if storeType != KeyValueStore {
		return NilResp, nil
	}
	v, ok := ts.tree.Get([]byte(k))
	if !ok {
		return NilResp, nil
	}
	return v.(string), nil
}

func (ts *TredsStore) MSet(kvs []string) error {
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
			err = ts.Set(keyValues[itr], keyValues[itr+1])
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

func (ts *TredsStore) MGet(args []string) ([]string, error) {
	results := make([]string, len(args))
	var g errgroup.Group
	var mu sync.Mutex
	for i, arg := range args {
		index := i
		key := arg
		g.Go(func() error {
			res, err := ts.Get(key)
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

func (ts *TredsStore) Set(k string, v string) error {
	kd := ts.getKeyDetails(k)
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
	ts.tree, _, _ = ts.tree.Insert([]byte(k), parsedArgs[0])
	return nil
}

func (ts *TredsStore) Delete(k string) error {
	ts.tree, _, _ = ts.tree.Delete([]byte(k))
	delete(ts.sortedMaps, k)
	delete(ts.sortedMapsScore, k)
	delete(ts.sortedMapsKeys, k)
	delete(ts.lists, k)
	delete(ts.sets, k)
	delete(ts.hashes, k)
	delete(ts.expiry, k)
	return nil
}

func (ts *TredsStore) PrefixScan(cursor, prefix, count string) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	iterator := ts.tree.Root().Iterator()
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
		if ts.hasExpired(string(key)) {
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

func (ts *TredsStore) PrefixScanKeys(cursor, prefix, count string) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	countInt, err := strconv.Atoi(count)
	if err != nil {
		return nil, err
	}
	iterator := ts.tree.Root().Iterator()
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
		if ts.hasExpired(string(key)) {
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

func (ts *TredsStore) DeletePrefix(prefix string) (int, error) {
	newTree, _, numDel := ts.tree.DeletePrefix([]byte(prefix))
	ts.tree = newTree
	return numDel, nil
}

func (ts *TredsStore) Keys(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	iterator := ts.tree.Root().Iterator()
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
		if ts.hasExpired(string(key)) {
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

func (ts *TredsStore) KeysH(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	rx := regexp.MustCompile(regex)

	seenHash := false
	if cursor == "0" {
		seenHash = true
	}
	nextCursor := uint32(0)

	result := make([]string, 0)

	keys := make([]string, 0)
	for key, _ := range ts.hashes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {

		if !rx.MatchString(key) {
			continue
		}

		if ts.hasExpired(key) {
			continue
		}
		hashKey, herr := hash(key)
		if herr != nil {
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && count > 0 {
			result = append(result, key)
			nextCursor, herr = hash(key)
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

func (ts *TredsStore) KeysL(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	rx := regexp.MustCompile(regex)

	seenHash := false
	if cursor == "0" {
		seenHash = true
	}
	nextCursor := uint32(0)

	result := make([]string, 0)

	keys := make([]string, 0)
	for key, _ := range ts.lists {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {

		if !rx.MatchString(key) {
			continue
		}

		if ts.hasExpired(key) {
			continue
		}
		hashKey, herr := hash(key)
		if herr != nil {
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && count > 0 {
			result = append(result, key)
			nextCursor, herr = hash(key)
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

func (ts *TredsStore) KeysS(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	rx := regexp.MustCompile(regex)

	seenHash := false
	if cursor == "0" {
		seenHash = true
	}
	nextCursor := uint32(0)

	result := make([]string, 0)

	keys := make([]string, 0)
	for key, _ := range ts.sets {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {

		if !rx.MatchString(key) {
			continue
		}

		if ts.hasExpired(key) {
			continue
		}
		hashKey, herr := hash(key)
		if herr != nil {
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && count > 0 {
			result = append(result, key)
			nextCursor, herr = hash(key)
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

func (ts *TredsStore) KeysZ(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	rx := regexp.MustCompile(regex)

	seenHash := false
	if cursor == "0" {
		seenHash = true
	}
	nextCursor := uint32(0)

	result := make([]string, 0)

	keys := make([]string, 0)
	for key, _ := range ts.sortedMapsScore {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {

		if !rx.MatchString(key) {
			continue
		}

		if ts.hasExpired(key) {
			continue
		}
		hashKey, herr := hash(key)
		if herr != nil {
			return nil, herr
		}
		if !seenHash && hashKey == uint32(startHash) {
			seenHash = true
			continue
		}
		if seenHash && count > 0 {
			result = append(result, key)
			nextCursor, herr = hash(key)
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

func (ts *TredsStore) KVS(cursor, regex string, count int) ([]string, error) {
	startHash, err := strconv.Atoi(cursor)
	if err != nil {
		return nil, err
	}
	iterator := ts.tree.Root().Iterator()
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
		if ts.hasExpired(string(key)) {
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

func (ts *TredsStore) Size() (int, error) {
	size := ts.tree.Len() + len(ts.sortedMaps) + len(ts.lists) + len(ts.sets) + len(ts.hashes)
	return size, nil
}

func (ts *TredsStore) ZAdd(args []string) error {
	kd := ts.getKeyDetails(args[0])
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
	if storedTm, ok := ts.sortedMaps[args[0]]; ok {
		tm = storedTm
	}
	sm := make(map[string]float64)
	if storedSm, ok := ts.sortedMapsScore[args[0]]; ok {
		sm = storedSm
	}
	sortedKeyMap, ok := ts.sortedMapsKeys[args[0]]
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
		_, radixTreeFloor := tm.Lower(score)
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
		_, radixTreeCeiling := tm.Greater(score)
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
	ts.sortedMaps[args[0]] = tm
	ts.sortedMapsScore[args[0]] = sm
	ts.sortedMapsKeys[args[0]] = sortedKeyMap
	return nil
}

func (ts *TredsStore) ZRem(args []string) error {
	kd := ts.getKeyDetails(args[0])
	if kd != -1 && kd != SortedMapStore {
		return fmt.Errorf("not sorted map store")
	}
	storedTm, ok := ts.sortedMaps[args[0]]
	if !ok {
		return nil
	}
	for itr := 1; itr < len(args); itr += 1 {
		key := []byte(args[itr])
		score, found := ts.sortedMapsScore[args[0]]
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
		_, radixTreeFloor := storedTm.Lower(scoreFloat)
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
		_, radixTreeCeiling := storedTm.Greater(scoreFloat)
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
	ts.sortedMaps[args[0]] = storedTm
	for _, arg := range args[1:] {
		delete(ts.sortedMapsScore[args[0]], arg)
		ts.sortedMapsKeys[args[0]], _, _ = ts.sortedMapsKeys[args[0]].Delete([]byte(arg))
	}
	return nil
}

func (ts *TredsStore) ZRangeByLexKVS(key, cursor, min, max, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	radixTree, ok := ts.sortedMapsKeys[key]
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
	sortedMapKey := ts.sortedMapsScore[key]
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

func (ts *TredsStore) ZRangeByLexKeys(key, cursor, min, max, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	radixTree, ok := ts.sortedMapsKeys[key]
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
	sortedMapKey := ts.sortedMapsScore[key]
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

func (ts *TredsStore) ZRangeByScoreKVS(key, min, max, offset, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	sortedMap := ts.sortedMaps[key]
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
	sortedMapKey := ts.sortedMapsScore[key]
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

func (ts *TredsStore) ZRangeByScoreKeys(key, min, max, offset, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	sortedMap := ts.sortedMaps[key]
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
	sortedMapKey := ts.sortedMapsScore[key]
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

func (ts *TredsStore) ZScore(args []string) (string, error) {
	kd := ts.getKeyDetails(args[0])
	if kd != -1 && kd != SortedMapStore {
		return "", fmt.Errorf("not sorted map store")
	}
	store, ok := ts.sortedMapsScore[args[0]]
	if !ok {
		return "", nil
	}
	if score, found := store[args[1]]; found {
		return strconv.FormatFloat(score, 'f', -1, 64), nil
	}
	return "", nil
}

func (ts *TredsStore) ZCard(key string) (int, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return 0, fmt.Errorf("not sorted map store")
	}
	store, ok := ts.sortedMapsKeys[key]
	if !ok {
		return 0, nil
	}
	return store.Len(), nil
}

func (ts *TredsStore) ZRevRangeByLexKVS(key, cursor, min, max, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	radixTree, ok := ts.sortedMapsKeys[key]
	if !ok {
		return nil, nil
	}
	iterator := radixTree.Root().ReverseIterator()
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
	sortedMapKey := ts.sortedMapsScore[key]
	for {
		storedKey, value, found := iterator.Previous()
		if !found {
			break
		}
		if index >= startIndex && countInt > 0 && strings.Compare(string(storedKey), min) >= 0 && strings.Compare(string(storedKey), max) <= 0 {
			if withScore {
				keyScore, _ := sortedMapKey[string(storedKey)]
				scoreStr := strconv.FormatFloat(keyScore, 'f', -1, 64)
				result = append(result, scoreStr)
				result = append(result, string(storedKey))
				result = append(result, value.(string))
			} else {
				result = append(result, string(storedKey))
				result = append(result, value.(string))
			}
			countInt--
		}
		if countInt == 0 || strings.Compare(string(storedKey), min) < 0 {
			break
		}
		index += 1
	}
	return result, nil
}

func (ts *TredsStore) ZRevRangeByLexKeys(key, cursor, min, max, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	radixTree, ok := ts.sortedMapsKeys[key]
	if !ok {
		return nil, nil
	}
	iterator := radixTree.Root().ReverseIterator()
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
	sortedMapKey := ts.sortedMapsScore[key]
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
				result = append(result, scoreStr)
				result = append(result, string(storedKey))
			} else {
				// Append only the storedKey to the result
				result = append(result, string(storedKey))
			}
			countInt--
		}
		if countInt == 0 || strings.Compare(string(storedKey), min) < 0 {
			break
		}
		index += 1
	}
	return result, nil
}

func (ts *TredsStore) ZRevRangeByScoreKVS(key, min, max, offset, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	sortedMap := ts.sortedMaps[key]
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
	_, radixTree := sortedMap.Floor(maxFloat)
	if radixTree == nil {
		return nil, nil
	}
	maxKV, _ := radixTree.(*radix_tree.Tree).Root().MaximumLeaf()
	sortedMapKey := ts.sortedMapsScore[key]
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
				result = append(result, scoreStr)
				result = append(result, string(maxKV.Key()))
				result = append(result, maxKV.Value().(string))
			} else {
				// Append only key and value to the result
				result = append(result, string(maxKV.Key()))
				result = append(result, maxKV.Value().(string))
			}
			countInt--
		}
		index++
		maxKV = maxKV.GetPrevLeaf()
	}
	return result, nil
}

func (ts *TredsStore) ZRevRangeByScoreKeys(key, min, max, offset, count string, withScore bool) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SortedMapStore {
		return nil, fmt.Errorf("not sorted map store")
	}
	sortedMap := ts.sortedMaps[key]
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
	_, radixTree := sortedMap.Floor(maxFloat)
	if radixTree == nil {
		return nil, nil
	}
	maxKV, _ := radixTree.(*radix_tree.Tree).Root().MaximumLeaf()
	sortedMapKey := ts.sortedMapsScore[key]
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
				result = append(result, scoreStr)
				result = append(result, string(maxKV.Key()))
			} else {
				// Append only key to the result
				result = append(result, string(maxKV.Key()))
			}
			countInt--
		}
		index++
		maxKV = maxKV.GetPrevLeaf()
	}
	return result, nil
}

func (ts *TredsStore) FlushAll() error {
	ts.tree = radix_tree.New()
	ts.sortedMaps = make(map[string]*treemap.Map)
	ts.sortedMapsScore = make(map[string]map[string]float64)
	ts.sortedMapsKeys = make(map[string]*radix_tree.Tree)
	ts.lists = make(map[string]*doublylinkedlist.List)
	ts.sets = make(map[string]*hashset.Set)
	ts.hashes = make(map[string]*hashmap.Map)
	ts.expiry = make(map[string]time.Time)
	return nil
}

func (ts *TredsStore) LPush(args []string) error {
	key := args[0]
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	validKey := validateKey(key)
	if !validKey {
		return fmt.Errorf("invalid key")
	}
	storedList, ok := ts.lists[key]
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
	ts.lists[key] = storedList
	return nil
}

func (ts *TredsStore) RPush(args []string) error {
	key := args[0]
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	validKey := validateKey(key)
	if !validKey {
		return fmt.Errorf("invalid key")
	}
	storedList, ok := ts.lists[key]
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
	ts.lists[key] = storedList
	return nil
}

func (ts *TredsStore) LIndex(args []string) (string, error) {
	key := args[0]
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return "", fmt.Errorf("not list store")
	}
	storedList, ok := ts.lists[key]
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

func (ts *TredsStore) LLen(key string) (int, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return 0, fmt.Errorf("not list store")
	}
	storedList, ok := ts.lists[key]
	if !ok {
		return 0, nil
	}
	return storedList.Size(), nil
}

func (ts *TredsStore) LRange(key string, start, stop int) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return nil, fmt.Errorf("not list store")
	}
	storedList, ok := ts.lists[key]
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

func (ts *TredsStore) LSet(key string, index int, element string) error {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	storedList, ok := ts.lists[key]
	if !ok {
		return nil
	}
	if index < 0 {
		index = storedList.Size() + index
	}
	storedList.Set(index, element)
	return nil
}

func (ts *TredsStore) LRem(key string, index int) error {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return fmt.Errorf("not list store")
	}
	storedList, ok := ts.lists[key]
	if !ok {
		return nil
	}
	if index < 0 {
		index = storedList.Size() + index
	}
	storedList.Remove(index)
	return nil
}

func (ts *TredsStore) LPop(key string, count int) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return nil, fmt.Errorf("not list store")
	}
	res := make([]string, 0)
	storedList, ok := ts.lists[key]
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

func (ts *TredsStore) RPop(key string, count int) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != ListStore {
		return nil, fmt.Errorf("not list store")
	}
	res := make([]string, 0)
	storedList, ok := ts.lists[key]
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

func (ts *TredsStore) SAdd(key string, members []string) error {
	kd := ts.getKeyDetails(key)
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
	storedSet, ok := ts.sets[key]
	if !ok {
		storedSet = hashset.New()
		ts.sets[key] = storedSet
	}
	for _, member := range parsedArgs {
		storedSet.Add(member)
	}
	return nil
}

func (ts *TredsStore) SRem(key string, members []string) error {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return fmt.Errorf("not set store")
	}
	parsedArgs, err := splitCommandWithQuotes(strings.Join(members, " "))
	if err != nil {
		return err
	}
	storedSet, ok := ts.sets[key]
	if !ok {
		return nil
	}
	for _, member := range parsedArgs {
		storedSet.Remove(member)
	}
	return nil
}

func (ts *TredsStore) SMembers(key string) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return nil, fmt.Errorf("not set store")
	}
	storedSet, ok := ts.sets[key]
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

func (ts *TredsStore) SIsMember(key string, member string) (bool, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return false, fmt.Errorf("not set store")
	}
	storedSet, ok := ts.sets[key]
	if !ok {
		return false, nil
	}
	return storedSet.Contains(member), nil
}

func (ts *TredsStore) SCard(key string) (int, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != SetStore {
		return 0, fmt.Errorf("not set store")
	}
	storedSet, ok := ts.sets[key]
	if !ok {
		return 0, nil
	}
	return storedSet.Size(), nil
}

func (ts *TredsStore) SUnion(keys []string) ([]string, error) {
	for _, key := range keys {
		kd := ts.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return nil, fmt.Errorf("not set store")
		}
	}
	unionSet := hashset.New()
	for _, key := range keys {
		storedSet, ok := ts.sets[key]
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

func (ts *TredsStore) SInter(keys []string) ([]string, error) {
	for _, key := range keys {
		kd := ts.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return nil, fmt.Errorf("not set store")
		}
	}
	intersectionSet := hashset.New()
	for _, key := range keys {
		storedSet, ok := ts.sets[key]
		if !ok {
			continue
		}
		intersectionSet = storedSet
		break
	}
	for _, key := range keys {
		storedSet, ok := ts.sets[key]
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

func (ts *TredsStore) SDiff(keys []string) ([]string, error) {
	for _, key := range keys {
		kd := ts.getKeyDetails(key)
		if kd != -1 && kd != SetStore {
			return nil, fmt.Errorf("not set store")
		}
	}
	diffSet, ok := ts.sets[keys[0]]
	if !ok {
		return nil, nil
	}
	for _, key := range keys[1:] {
		storedSet, found := ts.sets[key]
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

func (ts *TredsStore) HSet(key string, args []string) error {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return fmt.Errorf("not hash store")
	}
	validKey := validateKey(key)
	if !validKey {
		return fmt.Errorf("invalid key")
	}
	storedMap, ok := ts.hashes[key]
	if !ok {
		storedMap = hashmap.New()
		ts.hashes[key] = storedMap
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

func (ts *TredsStore) HGet(key string, field string) (string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return "", fmt.Errorf("not hash store")
	}
	storedMap, ok := ts.hashes[key]
	if !ok {
		return NilResp, nil
	}
	val, found := storedMap.Get(field)
	if !found {
		return NilResp, nil
	}
	return val.(string), nil
}

func (ts *TredsStore) HGetAll(key string) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return nil, fmt.Errorf("not hash store")
	}
	storedMap, ok := ts.hashes[key]
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
func (ts *TredsStore) HLen(key string) (int, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return 0, fmt.Errorf("not hash store")
	}
	storedMap, ok := ts.hashes[key]
	if !ok {
		return 0, nil
	}
	return storedMap.Size(), nil
}

func (ts *TredsStore) HDel(key string, fields []string) error {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return fmt.Errorf("not hash store")
	}
	storedMap, ok := ts.hashes[key]
	if !ok {
		return nil
	}
	for _, field := range fields {
		storedMap.Remove(field)
	}
	return nil
}

func (ts *TredsStore) HExists(key string, field string) (bool, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return false, fmt.Errorf("not hash store")
	}
	storedMap, ok := ts.hashes[key]
	if !ok {
		return false, nil
	}
	_, found := storedMap.Get(field)
	return found, nil
}

func (ts *TredsStore) HKeys(key string) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return nil, fmt.Errorf("not hash store")
	}
	storedMap, ok := ts.hashes[key]
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

func (ts *TredsStore) HVals(key string) ([]string, error) {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != HashStore {
		return nil, fmt.Errorf("not hash store")
	}
	storedMap, ok := ts.hashes[key]
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

func (ts *TredsStore) Expire(key string, expiration time.Time) error {
	ts.expiry[key] = expiration
	return nil
}

func (ts *TredsStore) Ttl(key string) int {
	if ts.getKeyStore(key) != -1 {
		if expiryTime, ok := ts.expiry[key]; ok {
			return int(expiryTime.Sub(time.Now()).Seconds())
		}
		return -1
	}
	return -2
}

func (ts *TredsStore) LongestPrefix(prefix string) ([]string, error) {
	res := make([]string, 0)
	key, val, found := ts.tree.Root().LongestPrefix([]byte(prefix))
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

func (ts *TredsStore) Snapshot() ([]byte, error) {
	// For now just persisting the root level key value store
	// That is tree *radix_tree.Tree in the Store
	store := &kvstore.KeyValueStore{
		Pairs: make([]*kvstore.KeyValue, 0),
	}
	minLeaf, found := ts.tree.Root().MinimumLeaf()
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

func (ts *TredsStore) Restore(data []byte) error {
	var deserializedStore kvstore.KeyValueStore
	err := proto.Unmarshal(data, &deserializedStore)
	if err != nil {
		fmt.Println("Error deserializing KeyValueStore:", err)
		return err
	}
	// Print the deserialized key-value pairs
	ts.tree = radix_tree.New()
	fmt.Println("Deserialized KeyValueStore:")
	for _, pair := range deserializedStore.Pairs {
		ts.tree, _, _ = ts.tree.Insert([]byte(pair.Key), pair.Value)
	}
	return nil
}

func (ts *TredsStore) DCreateCollection(args []string) error {
	collectionName := args[0]
	_, found := ts.collections[collectionName]
	if found {
		return fmt.Errorf("collection already exists")
	}
	collection := &Collection{
		Documents:       make(map[string]*Document),
		Indices:         make(map[string]*Index),
		Schema:          make(map[string]interface{}),
		DocumentIdIndex: make(map[string]map[string]struct{}),
	}
	if len(args) > 1 && args[1] != "" {
		jsonStr := args[1]
		err := json.Unmarshal([]byte(jsonStr), &collection.Schema)
		if err != nil {
			return err
		}
	}
	if len(args) > 2 && args[2] != "" {
		jsonStr := args[2]
		var indexes []map[string]interface{}
		err := json.Unmarshal([]byte(jsonStr), &indexes)
		if err != nil {
			return err
		}
		for _, index := range indexes {
			indexName := ""
			fields := index["fields"].([]interface{})
			fieldsString := make([]string, 0)
			for _, field := range fields {
				fieldsString = append(fieldsString, field.(string))
				indexName += field.(string) + "_"
			}
			indexName = strings.TrimSuffix(indexName, "_")
			indexName += IndexSuffix
			isUnique := false
			if index["type"] != nil && index["type"].(string) == Unique {
				isUnique = true
			}
			collection.Indices[indexName] = &Index{
				Fields: &CompoundKey{
					Fields: fieldsString,
				},
				isUnique: isUnique,
				indexer:  treemap.NewWith(CustomComparator),
			}
		}
	}
	ts.collections[collectionName] = collection
	return nil
}

func (ts *TredsStore) DDropCollection(args []string) error {
	collectionName := args[0]
	_, found := ts.collections[collectionName]
	if !found {
		return fmt.Errorf("collection does not exists")
	}
	delete(ts.collections, collectionName)
	return nil
}

func (ts *TredsStore) DInsert(args []string) (string, error) {
	collectionName := args[0]
	collection, foundCollection := ts.collections[collectionName]
	if !foundCollection {
		return "", fmt.Errorf("collection not found")
	}
	document := &Document{
		Id:         uuid.New().String(),
		StringData: "",
		Fields:     make(map[string]interface{}),
	}

	jsonStr := args[1]
	err := json.Unmarshal([]byte(jsonStr), &document.Fields)
	if err != nil {
		return "", err
	}
	document.Fields["_id"] = document.Id
	strData, err := json.Marshal(document.Fields)
	if err != nil {
		return "", err
	}
	document.StringData = string(strData)
	// Validate the document against the schema
	err = validateDocument(collection, document)
	if err != nil {
		return "", err
	}
	// Insert the document into the collection
	collection.Documents[document.Id] = document
	collection.DocumentIdIndex[document.Id] = make(map[string]struct{})
	// Insert the document into the indices
	for idx, index := range collection.Indices {
		treeMapKey := IndexValues{
			FieldValues: make([]interface{}, 0),
		}
		for _, field := range index.Fields.Fields {
			result := gjson.Get(document.StringData, field)
			treeMapKey.FieldValues = append(treeMapKey.FieldValues, getValue(result))
		}
		radixTree, found := index.indexer.Get(treeMapKey)
		if !found {
			radixTree = radix_tree.New()
		}
		storedRadixTree := radixTree.(*radix_tree.Tree)
		storedRadixTree, _, _ = storedRadixTree.Insert([]byte(document.Id), treeMapKey)
		index.indexer.Put(treeMapKey, storedRadixTree)
		collection.DocumentIdIndex[document.Id][idx] = struct{}{}

		// Linking the TreeMaps
		_, radixTreeLower := index.indexer.Lower(treeMapKey)
		if radixTreeLower != nil {
			lowerRadixTree := radixTreeLower.(*radix_tree.Tree)
			_, foundMinLeaf := lowerRadixTree.Root().MaximumLeaf()
			if foundMinLeaf {
				maxLeaf, foundMaxLeaf := lowerRadixTree.Root().MaximumLeaf()
				minLeaf, foundMinLeaf := storedRadixTree.Root().MinimumLeaf()
				if foundMaxLeaf {
					maxLeaf.SetNextLeaf(minLeaf)
				}
				if foundMinLeaf {
					minLeaf.SetPrevLeaf(maxLeaf)
				}
			}
		}
		_, radixTreeGreater := index.indexer.Greater(treeMapKey)
		if radixTreeGreater != nil {
			greaterRadixTree := radixTreeGreater.(*radix_tree.Tree)
			minLeaf, foundMaxLeaf := greaterRadixTree.Root().MinimumLeaf()
			maxLeaf, foundMinLeaf := storedRadixTree.Root().MaximumLeaf()
			if foundMaxLeaf {
				maxLeaf.SetNextLeaf(minLeaf)
			}
			if foundMinLeaf {
				minLeaf.SetPrevLeaf(maxLeaf)
			}
		}
		// storing the index
		collection.Indices[idx] = index
	}
	return document.Id, nil
}

func (ts *TredsStore) DExplain(query []string) (string, error) {
	collectionName := query[0]
	collection, foundCollection := ts.collections[collectionName]
	if !foundCollection {
		return "", fmt.Errorf("collection not found")
	}
	queryPlan := &Query{
		Filters: make([]QueryFilter, 0),
		Sort:    make([]Sort, 0),
		Limit:   0,
		Offset:  0,
	}
	jsonStr := query[1]
	err := json.Unmarshal([]byte(jsonStr), queryPlan)
	if err != nil {
		return "", err
	}
	if queryPlan.Limit == 0 {
		queryPlan.Limit = len(collection.Documents)
	}
	// Execute the query plan
	result := executeQueryPlan(collection, queryPlan)
	resultStr, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(resultStr), nil
}

func (ts *TredsStore) DQuery(query []string) ([]string, error) {
	collectionName := query[0]
	collection, foundCollection := ts.collections[collectionName]
	if !foundCollection {
		return nil, fmt.Errorf("collection not found")
	}
	queryPlan := &Query{
		Filters: make([]QueryFilter, 0),
		Sort:    make([]Sort, 0),
		Limit:   0,
		Offset:  0,
	}
	jsonStr := query[1]
	err := json.Unmarshal([]byte(jsonStr), queryPlan)
	if err != nil {
		return nil, err
	}
	if queryPlan.Limit == 0 {
		queryPlan.Limit = len(collection.Documents)
	}

	executionPlan := executeQueryPlan(collection, queryPlan)
	bestIndexName := ""
	lowestCost := len(collection.Documents)
	for indexName, indexData := range executionPlan {
		keysScan := indexData.(map[string]interface{})[TotalKeysExamined].(int)
		if keysScan < lowestCost {
			lowestCost = keysScan
			bestIndexName = indexName
		}
	}

	if bestIndexName == "" {
		filteredDocuments := fullScan(collection, queryPlan)
		jsonDocuments := make([]string, 0, len(filteredDocuments))
		for _, document := range filteredDocuments {
			jsonDocuments = append(jsonDocuments, document.StringData)
		}
		return jsonDocuments, nil
	}

	bestIndex := collection.Indices[bestIndexName]

	filteredResults := fetchAndFilterDocuments(collection, queryPlan, bestIndex)

	finalResults := applySortingAndPagination(filteredResults, queryPlan)

	jsonDocuments := make([]string, 0, len(finalResults))
	for _, document := range finalResults {
		jsonDocuments = append(jsonDocuments, document.StringData)
	}
	return jsonDocuments, nil
}

func (ts *TredsStore) VCreate(args []string) error {
	vectorName := args[0]
	_, found := ts.vectors[vectorName]
	if found {
		return fmt.Errorf("vector already exists")
	}
	maxNeighbor := 6
	levelFactor := 0.5
	efSearch := 20
	if len(args) > 1 {
		maxNeighbor, _ = strconv.Atoi(args[1])
	}
	if len(args) > 2 {
		parseLevelFactor, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return err
		}
		levelFactor = parseLevelFactor
	}
	if len(args) > 3 {
		effectiveSearch, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		efSearch = effectiveSearch
	}
	ts.vectors[vectorName] = hnsw.NewHNSW(maxNeighbor, levelFactor, efSearch, hnsw.EuclideanDistance)
	return nil
}

func (ts *TredsStore) VInsert(args []string) (string, error) {
	vectorName := args[0]
	vector, found := ts.vectors[vectorName]
	if !found {
		return "", fmt.Errorf("vector not found")
	}
	vectorData := make([]float64, 0)
	for _, data := range args[1:] {
		vectorDataFloat, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return "", err
		}
		vectorData = append(vectorData, vectorDataFloat)
	}
	return vector.Insert(vectorData), nil
}

func (ts *TredsStore) VSearch(args []string) ([][]string, error) {
	vectorName := args[0]
	vector, found := ts.vectors[vectorName]
	if !found {
		return nil, fmt.Errorf("vector not found")
	}
	vectorData := make([]float64, 0)
	for _, data := range args[1 : len(args)-1] {
		vectorDataFloat, err := strconv.ParseFloat(data, 64)
		if err != nil {
			return nil, err
		}
		vectorData = append(vectorData, vectorDataFloat)
	}
	k, err := strconv.Atoi(args[len(args)-1])
	if err != nil {
		return nil, err
	}
	results := vector.Search(vectorData, k)
	res := make([][]string, 0)
	for _, result := range results {
		resData := make([]string, 0)
		resData = append(resData, result.ID)
		for _, vecData := range result.Value {
			resData = append(resData, strconv.FormatFloat(vecData, 'f', -1, 64))
		}
		res = append(res, resData)
	}
	return res, nil
}

func (ts *TredsStore) VDelete(args []string) (bool, error) {
	vectorName := args[0]
	vector, found := ts.vectors[vectorName]
	if !found {
		return false, fmt.Errorf("vector not found")
	}
	if len(args) < 2 {
		return false, fmt.Errorf("vector data not provided")
	}
	nodeId := args[1]
	return vector.Delete(nodeId), nil
}

func (ts *TredsStore) BFAdd(key, field string) error {
	kd := ts.getKeyDetails(key)
	if kd != -1 && kd != BloomFilterStore {
		return fmt.Errorf("not bloom filter store")
	}
	bf, ok := ts.bloomFilters[key]
	if !ok {
		bf = bloom.NewWithEstimates(100000, 0.01)
	}
	bf.Add([]byte(field))
	ts.bloomFilters[key] = bf
	return nil
}
