package main

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	const key = "test-key"
	const value = "test-val"

	var val interface{}
	var contains bool

	defer delete(store.m, key)

	_, contains = store.m[key]
	if contains {
		t.Error("key/value alr exists")
	}

	//err should be nil
	err := Put(key, value)
	if err != nil {
		t.Error(err)
	}

	val, contains = store.m[key]
	if !contains {
		t.Error("create failed")
	}
	if val != value {
		t.Error("val/value mismatch")
	}

}

func TestGet(t *testing.T) {
	const key = "read-key"
	const value = "read-value"
	var err error

	var val interface{}
	defer delete(store.m, key)

	//Read a non-thing
	val, err = Get(key)
	if err == nil {
		t.Error("expected error on non-thing")
	}

	if !errors.Is(err, ErrorNoSuchKey) {
		t.Error("unexpected error: ", err)
	}

	store.m[key] = value

	val, err = Get(key)

	if val != value {
		t.Error("val/value mismatchs")
	}
	if err != nil {
		t.Error("unexpected error: ", err)
	}

}

func TestDelete(t *testing.T) {
	const key = "delete-key"
	const value = "delete-value"

	var contains bool
	defer delete(store.m, key)

	store.m[key] = value

	_, contains = store.m[key]
	if !contains {
		t.Error("key doesn't exist")
	}

	Delete(key)

	_, contains = store.m[key]
	if contains {
		t.Error("Delete failed")
	}
}
