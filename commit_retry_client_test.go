// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvmemdb"
)

// TestCommitRetryLostResponse simulates a commit whose success response is lost
// in transit: the server processes the commit but the client sees a transient
// failure. The client must retry and confirm success (spec §8.9, §11), and the
// value must be durably committed.
func TestCommitRetryLostResponse(t *testing.T) {
	ctx := context.Background()

	mdb := kvmemdb.New()
	real := Handler(kv.DatabaseFrom(mdb))

	var commitCalls int32
	flaky := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tx/commit" && atomic.AddInt32(&commitCalls, 1) == 1 {
			// Let the server actually process the commit, then drop the
			// response and report a transient failure to the client.
			real.ServeHTTP(httptest.NewRecorder(), r)
			http.Error(w, "simulated lost response", http.StatusServiceUnavailable)
			return
		}
		real.ServeHTTP(w, r)
	})

	dbServer := httptest.NewServer(flaky)
	defer dbServer.Close()

	dbURL, err := url.Parse(dbServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	db := kv.DatabaseFrom(New(dbURL, dbServer.Client()))

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "key1", strings.NewReader("value1")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit with a lost response = %v, want nil (confirmed committed)", err)
	}
	if n := atomic.LoadInt32(&commitCalls); n < 2 {
		t.Errorf("commit was attempted %d time(s), want a retry (>=2)", n)
	}

	// The write must be durably committed.
	var got string
	rerr := readValue(ctx, db, "key1", &got)
	if rerr != nil {
		t.Fatalf("read back: %v", rerr)
	}
	if got != "value1" {
		t.Errorf("committed value = %q, want %q", got, "value1")
	}
}

// TestCommitRetryContextDeadline verifies that indeterminate commit failures
// stop retrying when the caller's context expires, returning the context error.
func TestCommitRetryContextDeadline(t *testing.T) {
	ctx := context.Background()

	real := Handler(kv.DatabaseFrom(kvmemdb.New()))
	alwaysDown := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tx/commit" {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		real.ServeHTTP(w, r)
	})

	dbServer := httptest.NewServer(alwaysDown)
	defer dbServer.Close()

	dbURL, err := url.Parse(dbServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	db := kv.DatabaseFrom(New(dbURL, dbServer.Client()))

	tx, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = tx.Commit(cctx)
	if err == nil {
		t.Fatal("commit against a persistently failing server returned nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("commit error = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

func readValue(ctx context.Context, db kv.Database, key string, out *string) error {
	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		return err
	}
	defer snap.Discard(ctx)
	r, err := snap.Get(ctx, key)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	*out = string(data)
	return nil
}
