// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"context"
	"io"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvmemdb"
)

func TestReadWriteConflict(t *testing.T) {
	ctx := context.Background()

	dbServer := httptest.NewServer(Handler(kvmemdb.New()))
	defer dbServer.Close()

	dbURL, err := url.Parse(dbServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	db := New(dbURL, dbServer.Client())

	// Initialize with a key
	err = kv.WithReadWriter(ctx, db, func(ctx context.Context, rw kv.ReadWriter) error {
		if err := rw.Set(ctx, "key1", strings.NewReader("initial1")); err != nil {
			return err
		}
		if err := rw.Set(ctx, "key2", strings.NewReader("initial2")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to setup initial data: %v", err)
	}

	// Run two interleaved transactions accessing same key.

	tx1, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback(ctx)

	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback(ctx)

	if _, err := tx1.Get(ctx, "key1"); err != nil {
		t.Fatal(err)
	}

	if err := tx1.Set(ctx, "key2", strings.NewReader("value2")); err != nil {
		t.Fatal(err)
	}

	if err := tx2.Set(ctx, "key1", strings.NewReader("value1")); err != nil {
		t.Fatal(err)
	}

	if _, err := tx2.Get(ctx, "key2"); err != nil {
		t.Fatal(err)
	}

	err1 := tx1.Commit(ctx)
	err2 := tx2.Commit(ctx)

	// Verify only one transaction committed
	if err1 == nil && err2 == nil {
		t.Error("Both transactions committed, expected one to fail")
	}
	if err1 != nil && err2 != nil {
		t.Error("Both transactions failed, expected one to succeed")
	}

	// Check final state
	var finalKey1Value string
	var finalKey2Value string
	err = kv.WithReader(ctx, db, func(ctx context.Context, r kv.Reader) error {
		reader, err := r.Get(ctx, "key1")
		if err != nil {
			return err
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		finalKey1Value = string(data)

		reader, err = r.Get(ctx, "key2")
		if err != nil {
			return err
		}
		data, err = io.ReadAll(reader)
		if err != nil {
			return err
		}
		finalKey2Value = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to read final state: %v", err)
	}

	// Verify the final value matches the successful transaction
	if err1 == nil && (finalKey1Value != "initial1" || finalKey2Value != "value2") {
		t.Errorf("Final key1 value = %s, key2 value = %s want initial1 and value2 respectively", finalKey1Value, finalKey2Value)
	}
	if err2 == nil && (finalKey1Value != "value1" || finalKey2Value != "initial2") {
		t.Errorf("Final key1 value = %s, key2 value = %s want value1 and initial2 respectively", finalKey1Value, finalKey2Value)
	}
}
