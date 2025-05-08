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
	"github.com/visvasity/kv/kvutil"
	"github.com/visvasity/kvmemdb"
)

func TestWriteWriteSuccess(t *testing.T) {
	ctx := context.Background()

	mdb := kvmemdb.New()
	dbServer := httptest.NewServer(Handler(kv.DatabaseFrom(mdb.NewTransaction, mdb.NewSnapshot)))
	defer dbServer.Close()

	dbURL, err := url.Parse(dbServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	cdb := New(dbURL, dbServer.Client())
	db := kv.DatabaseFrom(cdb.NewTransaction, cdb.NewSnapshot)

	// Initialize with a key
	err = kvutil.WithReadWriter(ctx, db, func(ctx context.Context, rw kv.ReadWriter) error {
		return rw.Set(ctx, "key1", strings.NewReader("initial"))
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

	if err := tx1.Set(ctx, "key1", strings.NewReader("value1")); err != nil {
		t.Fatal(err)
	}

	if err := tx2.Set(ctx, "key1", strings.NewReader("value2")); err != nil {
		t.Fatal(err)
	}

	err1 := tx1.Commit(ctx)
	err2 := tx2.Commit(ctx)

	// Verify that both transactions are committed.
	if err1 != nil || err2 != nil {
		t.Error("Both transactions can commit")
	}

	// Check final state
	var finalValue string
	err = kvutil.WithReader(ctx, db, func(ctx context.Context, r kv.Reader) error {
		reader, err := r.Get(ctx, "key1")
		if err != nil {
			return err
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		finalValue = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to read final state: %v", err)
	}

	// Verify the final value matches the successful transaction
	if finalValue != "value1" && finalValue != "value2" {
		t.Errorf("Final value = %s, want value1 or value2", finalValue)
	}
}

func TestInterleavedBlindWrites(t *testing.T) {
	ctx := context.Background()

	mdb := kvmemdb.New()
	dbServer := httptest.NewServer(Handler(kv.DatabaseFrom(mdb.NewTransaction, mdb.NewSnapshot)))
	defer dbServer.Close()

	dbURL, err := url.Parse(dbServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	cdb := New(dbURL, dbServer.Client())
	db := kv.DatabaseFrom(cdb.NewTransaction, cdb.NewSnapshot)

	// Initialize with a key
	err = kvutil.WithReadWriter(ctx, db, func(ctx context.Context, rw kv.ReadWriter) error {
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

	if err := tx1.Set(ctx, "key1", strings.NewReader("value1-tx1")); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set(ctx, "key2", strings.NewReader("value2-tx2")); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Set(ctx, "key2", strings.NewReader("value2-tx1")); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Set(ctx, "key1", strings.NewReader("value2-tx2")); err != nil {
		t.Fatal(err)
	}

	err1 := tx1.Commit(ctx)
	err2 := tx2.Commit(ctx)

	// Verify that both transactions are committed.
	if err1 != nil || err2 != nil {
		t.Error("Both transactions can commit")
	}
}
