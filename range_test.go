// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kv/kvutil"
	"github.com/visvasity/kvmemdb"
)

func TestAscendDescend(t *testing.T) {
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

	// Setup test data.
	err = kvutil.WithReadWriter(ctx, db, func(ctx context.Context, rw kv.ReadWriter) error {
		if err := rw.Set(ctx, "key1", strings.NewReader("value1")); err != nil {
			return err
		}
		if err := rw.Set(ctx, "key2", strings.NewReader("value2")); err != nil {
			return err
		}
		if err := rw.Set(ctx, "key3", strings.NewReader("value3")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	tests := []struct {
		name     string
		beg, end string
		ascend   []string
		descend  []string
		wantErr  bool
	}{
		{
			name:    "Full range (empty beg and end)",
			beg:     "",
			end:     "",
			ascend:  []string{"key1", "key2", "key3"},
			descend: []string{"key3", "key2", "key1"},
			wantErr: false,
		},
		{
			name:    "From key1 to key3 (inclusive-exclusive)",
			beg:     "key1",
			end:     "key3",
			ascend:  []string{"key1", "key2"},
			descend: []string{"key2", "key1"},
			wantErr: false,
		},
		{
			name:    "From smallest key (empty beg)",
			beg:     "",
			end:     "key2",
			ascend:  []string{"key1"},
			descend: []string{"key1"},
			wantErr: false,
		},
		{
			name:    "To largest key (empty end)",
			beg:     "key2",
			end:     "",
			ascend:  []string{"key2", "key3"},
			descend: []string{"key3", "key2"},
			wantErr: false,
		},
		{
			name:    "Invalid range (beg >= end)",
			beg:     "key3",
			end:     "key1",
			ascend:  nil,
			descend: nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Ascend
			var ascendKeys []string
			var ascendErr error
			err = kvutil.WithReader(context.Background(), db, func(ctx context.Context, r kv.Reader) error {
				for k, v := range r.Ascend(ctx, tt.beg, tt.end, &ascendErr) {
					data, err := io.ReadAll(v)
					if err != nil {
						return err
					}
					if !strings.HasPrefix(k, "key") || !strings.HasPrefix(string(data), "value") {
						return errors.New("invalid key or value")
					}
					ascendKeys = append(ascendKeys, k)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("Ascend failed: %v", err)
			}
			if tt.wantErr && !errors.Is(ascendErr, os.ErrInvalid) {
				t.Errorf("Ascend expected error os.ErrInvalid, got %v", ascendErr)
			}
			if !tt.wantErr && ascendErr != nil {
				t.Errorf("Ascend unexpected error: %v", ascendErr)
			}
			if !reflect.DeepEqual(ascendKeys, tt.ascend) {
				t.Errorf("Ascend keys = %v, want %v", ascendKeys, tt.ascend)
			}

			// Test Descend
			var descendKeys []string
			var descendErr error
			err = kvutil.WithReader(context.Background(), db, func(ctx context.Context, r kv.Reader) error {
				for k, v := range r.Descend(ctx, tt.beg, tt.end, &descendErr) {
					data, err := io.ReadAll(v)
					if err != nil {
						return err
					}
					if !strings.HasPrefix(k, "key") || !strings.HasPrefix(string(data), "value") {
						return errors.New("invalid key or value")
					}
					descendKeys = append(descendKeys, k)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("Descend failed: %v", err)
			}
			if tt.wantErr && !errors.Is(descendErr, os.ErrInvalid) {
				t.Errorf("Descend expected error os.ErrInvalid, got %v", descendErr)
			}
			if !tt.wantErr && descendErr != nil {
				t.Errorf("Descend unexpected error: %v", descendErr)
			}
			if !reflect.DeepEqual(descendKeys, tt.descend) {
				t.Errorf("Descend keys = %v, want %v", descendKeys, tt.descend)
			}
		})
	}
}
