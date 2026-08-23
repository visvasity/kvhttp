// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"context"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvhttp/api"
	"github.com/visvasity/kvmemdb"
)

// TestCreateIdempotent verifies that a retried create for a name that already
// refers to a live object reports success rather than a conflict, so a create
// whose response was lost can be safely retried (spec §8.1, §8.2, §8.10,
// §8.11).
func TestCreateIdempotent(t *testing.T) {
	ctx := context.Background()

	t.Run("transaction", func(t *testing.T) {
		s := &server{db: kv.DatabaseFrom(kvmemdb.New())}
		if r, err := s.newTransaction(ctx, nil, &api.NewTransactionRequest{Name: "tx"}); err != nil || r.Error != "" {
			t.Fatalf("first create: err=%v resp=%+v", err, r)
		}
		r, err := s.newTransaction(ctx, nil, &api.NewTransactionRequest{Name: "tx"})
		if err != nil {
			t.Fatalf("retry create returned transport error: %v", err)
		}
		if r.Error != "" {
			t.Errorf("retry create: Error = %q, want empty (success)", r.Error)
		}
	})

	t.Run("snapshot", func(t *testing.T) {
		s := &server{db: kv.DatabaseFrom(kvmemdb.New())}
		if r, err := s.newSnapshot(ctx, nil, &api.NewSnapshotRequest{Name: "snap"}); err != nil || r.Error != "" {
			t.Fatalf("first create: err=%v resp=%+v", err, r)
		}
		r, err := s.newSnapshot(ctx, nil, &api.NewSnapshotRequest{Name: "snap"})
		if err != nil {
			t.Fatalf("retry create returned transport error: %v", err)
		}
		if r.Error != "" {
			t.Errorf("retry create: Error = %q, want empty (success)", r.Error)
		}
	})

	t.Run("iterator", func(t *testing.T) {
		s := &server{db: kv.DatabaseFrom(kvmemdb.New())}
		if _, err := s.newTransaction(ctx, nil, &api.NewTransactionRequest{Name: "tx"}); err != nil {
			t.Fatalf("newTransaction: %v", err)
		}
		req := &api.AscendRequest{Transaction: "tx", Name: "it"}
		if r, err := s.ascend(ctx, nil, req); err != nil || r.Error != "" {
			t.Fatalf("first ascend: err=%v resp=%+v", err, r)
		}
		r, err := s.ascend(ctx, nil, req)
		if err != nil {
			t.Fatalf("retry ascend returned transport error: %v", err)
		}
		if r.Error != "" {
			t.Errorf("retry ascend: Error = %q, want empty (success)", r.Error)
		}
	})
}
