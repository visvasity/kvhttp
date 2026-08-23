// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"context"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvmemdb"
)

// TestClientMapsNotFound verifies that a 404 (unknown name) reported by the
// server is surfaced to the client as os.ErrNotExist, so callers can match it
// with errors.Is (spec §7.1, §11).
func TestClientMapsNotFound(t *testing.T) {
	ctx := context.Background()

	dbServer := httptest.NewServer(Handler(kv.DatabaseFrom(kvmemdb.New())))
	defer dbServer.Close()

	dbURL, err := url.Parse(dbServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	cdb := New(dbURL, dbServer.Client())

	// A transaction whose name was never registered on the server yields 404.
	tx := &Tx{db: cdb, id: "does-not-exist"}
	if _, err := tx.Get(ctx, "key"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Get on unknown transaction = %v, want os.ErrNotExist", err)
	}
}
