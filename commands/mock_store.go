package commands

import (
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MockStore is a mock implementation of the store interface for testing.
type MockStore struct {
	data map[string]string
}

func (m *MockStore) Get(key string) (string, error) {
	val, exists := m.data[key]
	if !exists {
		return "(nil)", errors.New("key does not exist")
	}
	return val, nil
}

func (m *MockStore) MGet(keys []string) ([]string, error) {
	res := make([]string, 0)
	for _, key := range keys {
		val, _ := m.Get(key)
		res = append(res, val)
	}
	return res, nil
}

func (m *MockStore) MSet(keys []string) error {
	return nil
}

func (m *MockStore) Set(key, value string) error {
	m.data[key] = value
	return nil
}

func (m *MockStore) Delete(key string) error {
	if _, exists := m.data[key]; !exists {
		return errors.New("key does not exist")
	}
	delete(m.data, key)
	return nil
}

func (m *MockStore) PrefixScanKeys(cursor, prefix, count string) ([]string, error) {
	res := make([]string, 0)
	keys := make([]string, 0)
	for key, _ := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	countInt, _ := strconv.Atoi(count)
	cursorInt, _ := strconv.Atoi(cursor)
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) && countInt > 0 {
			if cursorInt > 0 {
				cursorInt--
				continue
			}
			res = append(res, key)
			countInt--
		}
	}
	return res, nil
}

func (m *MockStore) PrefixScan(cursor, prefix, count string) ([]string, error) {
	res := make([]string, 0)
	keys := make([]string, 0)
	for key, _ := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	countInt, _ := strconv.Atoi(count)
	cursorInt, _ := strconv.Atoi(cursor)
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) && countInt > 0 {
			if cursorInt > 0 {
				cursorInt--
				continue
			}
			res = append(res, key)
			res = append(res, m.data[key])
			countInt--
		}
	}
	return res, nil
}

func (m *MockStore) DeletePrefix(prefix string) (int, error) {
	return 0, nil
}

func (m *MockStore) Keys(cursor, regex string, count int) ([]string, error) {
	res := make([]string, 0)
	keys := make([]string, 0)
	for key, _ := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		match, _ := regexp.MatchString(regex, key)
		if match {
			res = append(res, key)
		}
	}
	return res, nil
}

func (m *MockStore) KVS(cursor, regex string, count int) ([]string, error) {
	res := make([]string, 0)
	keys := make([]string, 0)
	for key, _ := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		match, _ := regexp.MatchString(regex, key)
		if match {
			res = append(res, key)
			res = append(res, m.data[key])
		}
	}
	return res, nil
}

func (m *MockStore) Size() (int, error) {
	return 0, nil
}

func (m *MockStore) ZAdd([]string) error {
	return nil
}

func (m *MockStore) ZRem([]string) error {
	return nil
}

func (m *MockStore) ZRangeByLexKVS(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}
func (m *MockStore) ZRangeByLexKeys(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}

func (m *MockStore) ZRangeByScoreKeys(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}

func (m *MockStore) ZRangeByScoreKVS(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}
func (m *MockStore) ZScore([]string) (string, error) {
	return "", nil
}

func (m *MockStore) ZCard(string) (int, error) {
	return 0, nil
}

func (m *MockStore) ZRevRangeByLexKVS(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}
func (m *MockStore) ZRevRangeByLexKeys(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}

func (m *MockStore) ZRevRangeByScoreKeys(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}

func (m *MockStore) ZRevRangeByScoreKVS(string, string, string, string, string, bool) ([]string, error) {
	return nil, nil
}

func (m *MockStore) FlushAll() error {
	return nil
}

func (m *MockStore) LPush([]string) error {
	return nil
}

func (m *MockStore) RPush([]string) error {
	return nil
}

func (rs *MockStore) LIndex(args []string) (string, error) {
	return "", nil
}

func (rs *MockStore) LLen(key string) (string, error) {
	return "", nil
}

func (rs *MockStore) LRange(key string, start, stop int) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) LSet(key string, index int, element string) error {
	return nil
}

func (rs *MockStore) LRem(key string, index int) error {
	return nil
}

func (rs *MockStore) LPop(key string, count int) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) RPop(key string, count int) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) SAdd(key string, members []string) error {
	return nil
}

func (rs *MockStore) SRem(key string, members []string) error {
	return nil
}

func (rs *MockStore) SMembers(key string) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) SIsMember(key string, member string) (bool, error) {
	return false, nil
}

func (rs *MockStore) SCard(key string) (int, error) {
	return 0, nil
}

func (rs *MockStore) SUnion(keys []string) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) SInter(keys []string) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) SDiff(keys []string) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) HSet(key string, args []string) error {
	return nil
}

func (rs *MockStore) HGet(key string, field string) (string, error) {
	return "", nil
}

func (rs *MockStore) HGetAll(key string) ([]string, error) {
	return nil, nil
}
func (rs *MockStore) HLen(key string) (int, error) {
	return 0, nil
}

func (rs *MockStore) HDel(key string, fields []string) error {
	return nil
}

func (rs *MockStore) HExists(key string, field string) (bool, error) {
	return false, nil
}

func (rs *MockStore) HKeys(key string) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) HVals(key string) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) CleanUpExpiredKeys() {
}

func (rs *MockStore) Expire(key string, expiration time.Time) error {
	return nil
}

func (rs *MockStore) Ttl(key string) int {
	return 0
}

func (rs *MockStore) LongestPrefix(key string) ([]string, error) {
	return nil, nil
}

func (rs *MockStore) Snapshot() ([]byte, error) {
	return nil, nil
}

func (rs *MockStore) Restore(data []byte) error {
	return nil
}
