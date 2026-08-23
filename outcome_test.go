// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"context"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvhttp/api"
	"github.com/visvasity/kvmemdb"
)

// TestCommitRecordsOutcome verifies that a transaction's terminal outcome
// (committed vs rolled back) is recorded when the transaction ends, so that
// retried commit/rollback requests can learn the decided result (spec §8.9).
func TestCommitRecordsOutcome(t *testing.T) {
	ctx := context.Background()

	newServer := func() *server {
		return &server{db: kv.DatabaseFrom(kvmemdb.New())}
	}

	t.Run("committed", func(t *testing.T) {
		s := newServer()
		if _, err := s.newTransaction(ctx, nil, &api.NewTransactionRequest{Name: "tx1"}); err != nil {
			t.Fatalf("newTransaction: %v", err)
		}
		resp, err := s.commit(ctx, nil, &api.CommitRequest{Transaction: "tx1"})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if resp.Error != "" {
			t.Fatalf("commit reported error: %q", resp.Error)
		}
		ci, ok := s.closedMap.Load("tx1")
		if !ok {
			t.Fatal("tx1 not recorded as closed after commit")
		}
		if ci.outcome != outcomeCommitted {
			t.Errorf("outcome = %v, want outcomeCommitted", ci.outcome)
		}
	})

	t.Run("rolled-back", func(t *testing.T) {
		s := newServer()
		if _, err := s.newTransaction(ctx, nil, &api.NewTransactionRequest{Name: "tx2"}); err != nil {
			t.Fatalf("newTransaction: %v", err)
		}
		if _, err := s.rollback(ctx, nil, &api.RollbackRequest{Transaction: "tx2"}); err != nil {
			t.Fatalf("rollback: %v", err)
		}
		ci, ok := s.closedMap.Load("tx2")
		if !ok {
			t.Fatal("tx2 not recorded as closed after rollback")
		}
		if ci.outcome != outcomeRolledBack {
			t.Errorf("outcome = %v, want outcomeRolledBack", ci.outcome)
		}
	})
}
