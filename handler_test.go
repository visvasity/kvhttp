// Copyright (c) 2025 Visvasity LLC

package kvhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/visvasity/kv"
	"github.com/visvasity/kvmemdb"
)

// TestBadRequestBody verifies that requests the handler cannot decode into a
// request object are rejected with 400 Bad Request rather than crashing or
// returning 500 (spec §2.2, §7.1).
func TestBadRequestBody(t *testing.T) {
	dbServer := httptest.NewServer(Handler(kv.DatabaseFrom(kvmemdb.New())))
	defer dbServer.Close()

	cases := []struct {
		name string
		body *strings.Reader
	}{
		{name: "empty body", body: strings.NewReader("")},
		{name: "malformed json", body: strings.NewReader("{not json")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(dbServer.URL+"/new-transaction", "application/json", tc.body)
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}
