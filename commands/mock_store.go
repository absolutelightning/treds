package commands

import (
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

func (m *MockStore) MGet(keys []string) (string, error) {
	res := ""
	for _, key := range keys {
		val, _ := m.Get(key)
		res += val + "\n"
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

func (m *MockStore) PrefixScanKeys(cursor, prefix, count string) (string, error) {
	res := ""
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
			res += key + "\n"
			countInt--
		}
	}
	return res, nil
}

func (m *MockStore) PrefixScan(cursor, prefix, count string) (string, error) {
	res := ""
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
			res += key + "\n" + m.data[key] + "\n"
			countInt--
		}
	}
	return res, nil
}

func (m *MockStore) DeletePrefix(prefix string) error {
	return nil
}

func (m *MockStore) Keys(regex string) (string, error) {
	res := ""
	keys := make([]string, 0)
	for key, _ := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		match, _ := regexp.MatchString(regex, key)
		if match {
			res += key + "\n"
		}
	}
	return res, nil
}

func (m *MockStore) KVS(regex string) (string, error) {
	res := ""
	keys := make([]string, 0)
	for key, _ := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		match, _ := regexp.MatchString(regex, key)
		if match {
			res += key + "\n" + m.data[key] + "\n"
		}
	}
	return res, nil
}

func (m *MockStore) Size() (string, error) {
	return "", nil
}

func (m *MockStore) ZAdd([]string) error {
	return nil
}

func (m *MockStore) ZRem([]string) error {
	return nil
}

func (m *MockStore) ZRangeByLexKVS(string, string, string, string, bool) (string, error) {
	return "", nil
}
func (m *MockStore) ZRangeByLexKeys(string, string, string, string, bool) (string, error) {
	return "", nil
}

func (m *MockStore) ZRangeByScoreKeys(string, string, string, string, string, bool) (string, error) {
	return "", nil
}

func (m *MockStore) ZRangeByScoreKVS(string, string, string, string, string, bool) (string, error) {
	return "", nil
}
func (m *MockStore) ZScore([]string) (string, error) {
	return "", nil
}

func (m *MockStore) ZCard(string) (int, error) {
	return 0, nil
}

func (m *MockStore) ZRevRangeByLexKVS(string, string, string, string, bool) (string, error) {
	return "", nil
}
func (m *MockStore) ZRevRangeByLexKeys(string, string, string, string, bool) (string, error) {
	return "", nil
}

func (m *MockStore) ZRevRangeByScoreKeys(string, string, string, string, string, bool) (string, error) {
	return "", nil
}

func (m *MockStore) ZRevRangeByScoreKVS(string, string, string, string, string, bool) (string, error) {
	return "", nil
}

func (m *MockStore) FlushAll() error {
	return nil
}
