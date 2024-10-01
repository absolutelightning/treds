package store

import (
	"testing"
)

func TestTredsStore_Get(t *testing.T) {
	store := NewTredsStore()

	// Test getting a non-existent key
	value, err := store.Get("nonexistent")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != NilResp {
		t.Fatalf("expected %s, got %s", NilResp, value)
	}

	// Test setting and then getting a key
	store.Set("key1", "value1")
	value, err = store.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != "value1\n" {
		t.Fatalf("expected value1, got %s", value)
	}
}

func TestTredsStore_MGet(t *testing.T) {
	store := NewTredsStore()

	store.Set("key1", "value1")
	store.Set("key2", "value2")

	// Test getting multiple keys
	values, err := store.MGet([]string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "value1\nvalue2\n(nil)\n"
	if values != expected {
		t.Fatalf("expected %s, got %s", expected, values)
	}
}

func TestTredsStore_Set(t *testing.T) {
	store := NewTredsStore()

	// Test setting and then getting a key
	err := store.Set("key1", "value1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	value, err := store.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != "value1\n" {
		t.Fatalf("expected value1, got %s", value)
	}
}

func TestTredsStore_Delete(t *testing.T) {
	store := NewTredsStore()

	store.Set("key1", "value1")

	// Test deleting an existing key
	err := store.Delete("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	value, err := store.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != NilResp {
		t.Fatalf("expected %s, got %s", NilResp, value)
	}

	// Test deleting a non-existent key
	err = store.Delete("nonexistent")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestTredsStore_PrefixScan(t *testing.T) {
	store := NewTredsStore()

	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("key3", "value3")

	// Test prefix scan
	result, err := store.PrefixScan("0", "key", "2")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "key1\nvalue1\nkey2\nvalue2\n944401402\n"
	if result != expected {
		t.Fatalf("expected %s, got %s", expected, result)
	}
}

func TestTredsStore_PrefixScanKeys(t *testing.T) {
	store := NewTredsStore()

	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("key3", "value3")

	// Test prefix scan keys
	result, err := store.PrefixScanKeys("0", "key", "2")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "key1\nkey2\n944401402\n"
	if result != expected {
		t.Fatalf("expected %s, got %s", expected, result)
	}
}

func TestTredsStore_DeletePrefix(t *testing.T) {
	store := NewTredsStore()

	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("other", "value3")

	// Test deleting with a prefix
	_, err := store.DeletePrefix("key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	value, err := store.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != NilResp {
		t.Fatalf("expected %s, got %s", NilResp, value)
	}

	value, err = store.Get("other")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if value != "value3\n" {
		t.Fatalf("expected value3, got %s", value)
	}
}

func TestTredsStore_Keys(t *testing.T) {
	store := NewTredsStore()

	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("otherkey", "value3")

	// Test retrieving keys matching a regex
	result, err := store.Keys("0", "^key.*", 100000000)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "key1\nkey2\n0\n"
	if result != expected {
		t.Fatalf("expected %s, got %s", expected, result)
	}
}

func TestTredsStore_KVS(t *testing.T) {
	store := NewTredsStore()

	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("otherkey", "value3")

	// Test retrieving key-value pairs matching a regex
	result, err := store.KVS("0", "^key.*", 10000000000000)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	expected := "key1\nvalue1\nkey2\nvalue2\n0\n"
	if result != expected {
		t.Fatalf("expected %s, got %s", expected, result)
	}
}
