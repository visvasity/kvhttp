// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"context"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvhttp/api"
	"github.com/visvasity/kvmemdb"
)

// TestCommitRetryOutcome verifies the retry-to-confirm commit contract
// (spec §8.9): a retried commit of an already-committed transaction reports
// success (so a lost commit response never surfaces as failure), while a
// retried commit of a rolled-back transaction reports ErrClosed.
func TestCommitRetryOutcome(t *testing.T) {
	ctx := context.Background()

	t.Run("committed-then-commit", func(t *testing.T) {
		s := &server{db: kv.DatabaseFrom(kvmemdb.New())}
		if _, err := s.newTransaction(ctx, nil, &api.NewTransactionRequest{Name: "tx1"}); err != nil {
			t.Fatalf("newTransaction: %v", err)
		}
		if resp, err := s.commit(ctx, nil, &api.CommitRequest{Transaction: "tx1"}); err != nil || resp.Error != "" {
			t.Fatalf("first commit: err=%v resp=%+v", err, resp)
		}
		resp, err := s.commit(ctx, nil, &api.CommitRequest{Transaction: "tx1"})
		if err != nil {
			t.Fatalf("retry commit returned transport error: %v", err)
		}
		if resp.Error != "" {
			t.Errorf("retry commit of committed tx: Error = %q, want empty (success)", resp.Error)
		}
	})

	t.Run("rolledback-then-commit", func(t *testing.T) {
		s := &server{db: kv.DatabaseFrom(kvmemdb.New())}
		if _, err := s.newTransaction(ctx, nil, &api.NewTransactionRequest{Name: "tx2"}); err != nil {
			t.Fatalf("newTransaction: %v", err)
		}
		if _, err := s.rollback(ctx, nil, &api.RollbackRequest{Transaction: "tx2"}); err != nil {
			t.Fatalf("rollback: %v", err)
		}
		resp, err := s.commit(ctx, nil, &api.CommitRequest{Transaction: "tx2"})
		if err != nil {
			t.Fatalf("commit after rollback returned transport error: %v", err)
		}
		if resp.Error != "ErrClosed" {
			t.Errorf("commit of rolled-back tx: Error = %q, want ErrClosed", resp.Error)
		}
	})
}

// TestCommitRetryEndToEnd exercises the retry-to-confirm behavior through the
// full HTTP client/server stack: committing the same transaction twice via the
// client must both succeed (spec §8.9, §11).
func TestCommitRetryEndToEnd(t *testing.T) {
	ctx := context.Background()

	mdb := kvmemdb.New()
	dbServer := httptest.NewServer(Handler(kv.DatabaseFrom(mdb)))
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
		t.Fatalf("first commit: %v", err)
	}
	// A retried commit (e.g. after a lost response) must not report failure.
	if err := tx.Commit(ctx); err != nil {
		t.Errorf("retry commit returned %v, want nil (success)", err)
	}

	// A commit after an explicit rollback must report ErrClosed.
	tx2, err := db.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); !errors.Is(err, os.ErrClosed) {
		t.Errorf("commit after rollback returned %v, want os.ErrClosed", err)
	}
}
