// Copyright (c) 2024 Visvasity LLC

package kvhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/google/uuid"

	"github.com/visvasity/kvhttp/api"
)

// commit retry-to-confirm tuning (spec §8.9). An indeterminate commit failure
// (network error, timeout, 5xx) is retried with capped exponential backoff
// until the caller's context expires or a definite outcome is observed.
const (
	commitRetryBaseDelay = 50 * time.Millisecond
	commitRetryMaxDelay  = 5 * time.Second
)

// httpStatusError reports a non-OK HTTP status from the server, preserving the
// status code so callers (e.g. commit retry) can distinguish transient 5xx
// failures from deterministic 4xx ones.
type httpStatusError struct {
	code int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("received non-ok http status %d", e.code)
}

type DB struct {
	dbURL url.URL

	httpClient *http.Client

	closecalls []func()
}

type Tx struct {
	db *DB
	id string
}

type Snap struct {
	db *DB
	id string
}

func New(baseURL *url.URL, client *http.Client) *DB {
	if client == nil {
		client = http.DefaultClient
	}

	db := &DB{
		httpClient: client,
		dbURL: url.URL{
			Host:   baseURL.Host,
			Scheme: baseURL.Scheme,
			Path:   baseURL.Path,
		},
	}
	return db
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) ServerURL() url.URL {
	return db.dbURL
}

func (db *DB) NewTransaction(ctx context.Context) (*Tx, error) {
	id := uuid.New().String()
	resp, err := doPost[api.NewTransactionResponse](ctx, db, "/new-transaction", &api.NewTransactionRequest{Name: id})
	if err != nil {
		return nil, err
	}
	if len(resp.Error) != 0 {
		return nil, string2error(resp.Error)
	}
	return &Tx{db: db, id: id}, nil
}

func (db *DB) NewSnapshot(ctx context.Context) (*Snap, error) {
	id := uuid.New().String()
	resp, err := doPost[api.NewSnapshotResponse](ctx, db, "/new-snapshot", &api.NewSnapshotRequest{Name: id})
	if err != nil {
		return nil, err
	}
	if len(resp.Error) != 0 {
		return nil, string2error(resp.Error)
	}
	return &Snap{db: db, id: id}, nil
}

func (tx *Tx) Get(ctx context.Context, key string) (io.Reader, error) {
	req := &api.GetRequest{Transaction: tx.id, Key: []byte(key)}
	resp, err := doPost[api.GetResponse](ctx, tx.db, "/tx/get", req)
	if err != nil {
		return nil, err
	}
	if len(resp.Error) != 0 {
		return nil, string2error(resp.Error)
	}
	return bytes.NewReader(resp.Value), nil
}

func (tx *Tx) Set(ctx context.Context, key string, value io.Reader) error {
	if value == nil {
		return os.ErrInvalid
	}
	data, err := io.ReadAll(value)
	if err != nil {
		return err
	}
	req := &api.SetRequest{
		Transaction: tx.id,
		Key:         []byte(key),
		Value:       data,
	}
	resp, err := doPost[api.SetResponse](ctx, tx.db, "/tx/set", req)
	if err != nil {
		return err
	}
	if len(resp.Error) != 0 {
		return string2error(resp.Error)
	}
	return nil
}

func (tx *Tx) Delete(ctx context.Context, key string) error {
	req := &api.DeleteRequest{Transaction: tx.id, Key: []byte(key)}
	resp, err := doPost[api.DeleteResponse](ctx, tx.db, "/tx/delete", req)
	if err != nil {
		return err
	}
	if len(resp.Error) != 0 {
		return string2error(resp.Error)
	}
	return nil
}

func (tx *Tx) Ascend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		req1 := &api.AscendRequest{
			Transaction: tx.id,
			Name:        uuid.New().String(),
			Begin:       []byte(begin),
			End:         []byte(end),
		}
		resp1, err := doPost[api.AscendResponse](ctx, tx.db, "/tx/ascend", req1)
		if err != nil {
			*errp = err
			return
		}
		if len(resp1.Error) != 0 {
			*errp = string2error(resp1.Error)
			return
		}

		for {
			req2 := &api.NextRequest{Iterator: req1.Name}
			resp2, err := doPost[api.NextResponse](ctx, tx.db, "/it/next", req2)
			if err != nil {
				*errp = err
				return
			}
			if len(resp2.Error) != 0 {
				*errp = string2error(resp2.Error)
				return
			}
			if len(resp2.Key) == 0 {
				return // EOF
			}
			if !yield(string(resp2.Key), bytes.NewReader(resp2.Value)) {
				return
			}
		}
	}
}

func (tx *Tx) Descend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		req1 := &api.DescendRequest{
			Transaction: tx.id,
			Name:        uuid.New().String(),
			Begin:       []byte(begin),
			End:         []byte(end),
		}
		resp1, err := doPost[api.DescendResponse](ctx, tx.db, "/tx/descend", req1)
		if err != nil {
			*errp = err
			return
		}
		if len(resp1.Error) != 0 {
			*errp = string2error(resp1.Error)
			return
		}

		for {
			req2 := &api.NextRequest{Iterator: req1.Name}
			resp2, err := doPost[api.NextResponse](ctx, tx.db, "/it/next", req2)
			if err != nil {
				*errp = err
				return
			}
			if len(resp2.Error) != 0 {
				*errp = string2error(resp2.Error)
				return
			}
			if len(resp2.Key) == 0 {
				return // EOF
			}
			if !yield(string(resp2.Key), bytes.NewReader(resp2.Value)) {
				return
			}
		}
	}
}

func (tx *Tx) Commit(ctx context.Context) error {
	req := &api.CommitRequest{Transaction: tx.id}
	// Retry-to-confirm (spec §8.9): an indeterminate failure leaves the commit
	// in doubt, so re-send until the server reports a definite outcome. The
	// server makes a replayed commit idempotent -- a committed transaction
	// reports success, a rolled-back one reports ErrClosed -- so retrying never
	// double-applies or falsely reports failure.
	for attempt := 0; ; attempt++ {
		resp, err := doPost[api.CommitResponse](ctx, tx.db, "/tx/commit", req)
		if err == nil {
			if len(resp.Error) != 0 {
				return string2error(resp.Error)
			}
			return nil
		}
		if !isRetryableCommitErr(err) {
			return err
		}
		if werr := waitBackoff(ctx, attempt); werr != nil {
			// The context expired before we could confirm the outcome; report
			// the last transport error together with the context error.
			return errors.Join(err, werr)
		}
	}
}

// isRetryableCommitErr reports whether a commit failure is indeterminate and so
// should be retried to confirm the outcome (spec §8.9). Well-formed responses
// are handled by the caller; here only transport failures are seen. A 404
// (os.ErrNotExist) and other 4xx statuses are deterministic and not retried;
// 5xx and network/timeout errors are transient.
func isRetryableCommitErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	var se *httpStatusError
	if errors.As(err, &se) {
		return se.code >= 500
	}
	return true
}

// waitBackoff sleeps for the attempt's backoff delay, returning the context
// error if it is cancelled or expires first.
func waitBackoff(ctx context.Context, attempt int) error {
	t := time.NewTimer(backoffDelay(attempt))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func backoffDelay(attempt int) time.Duration {
	if attempt < 0 || attempt > 20 {
		return commitRetryMaxDelay
	}
	d := commitRetryBaseDelay << uint(attempt)
	if d <= 0 || d > commitRetryMaxDelay {
		return commitRetryMaxDelay
	}
	return d
}

func (tx *Tx) Rollback(ctx context.Context) error {
	req := &api.RollbackRequest{Transaction: tx.id}
	resp, err := doPost[api.RollbackResponse](ctx, tx.db, "/tx/rollback", req)
	if err != nil {
		return err
	}
	if len(resp.Error) != 0 {
		return string2error(resp.Error)
	}
	return nil
}

func (snap *Snap) Get(ctx context.Context, key string) (io.Reader, error) {
	req := &api.GetRequest{Snapshot: snap.id, Key: []byte(key)}
	resp, err := doPost[api.GetResponse](ctx, snap.db, "/snap/get", req)
	if err != nil {
		return nil, err
	}
	if len(resp.Error) != 0 {
		return nil, string2error(resp.Error)
	}
	return bytes.NewReader(resp.Value), nil
}

func (snap *Snap) Ascend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		req1 := &api.AscendRequest{
			Snapshot: snap.id,
			Name:     uuid.New().String(),
			Begin:    []byte(begin),
			End:      []byte(end),
		}
		resp1, err := doPost[api.AscendResponse](ctx, snap.db, "/snap/ascend", req1)
		if err != nil {
			*errp = err
			return
		}
		if len(resp1.Error) != 0 {
			*errp = string2error(resp1.Error)
			return
		}

		for {
			req2 := &api.NextRequest{Iterator: req1.Name}
			resp2, err := doPost[api.NextResponse](ctx, snap.db, "/it/next", req2)
			if err != nil {
				*errp = err
				return
			}
			if len(resp2.Error) != 0 {
				*errp = string2error(resp2.Error)
				return
			}
			if len(resp2.Key) == 0 {
				return // EOF
			}
			if !yield(string(resp2.Key), bytes.NewReader(resp2.Value)) {
				return
			}
		}
	}
}

func (snap *Snap) Descend(ctx context.Context, begin, end string, errp *error) iter.Seq2[string, io.Reader] {
	return func(yield func(string, io.Reader) bool) {
		req1 := &api.DescendRequest{
			Snapshot: snap.id,
			Name:     uuid.New().String(),
			Begin:    []byte(begin),
			End:      []byte(end),
		}
		resp1, err := doPost[api.DescendResponse](ctx, snap.db, "/snap/descend", req1)
		if err != nil {
			*errp = err
			return
		}
		if len(resp1.Error) != 0 {
			*errp = string2error(resp1.Error)
			return
		}

		for {
			req2 := &api.NextRequest{Iterator: req1.Name}
			resp2, err := doPost[api.NextResponse](ctx, snap.db, "/it/next", req2)
			if err != nil {
				*errp = err
				return
			}
			if len(resp2.Error) != 0 {
				*errp = string2error(resp2.Error)
				return
			}
			if len(resp2.Key) == 0 {
				return // EOF
			}
			if !yield(string(resp2.Key), bytes.NewReader(resp2.Value)) {
				return
			}
		}
	}
}

func (snap *Snap) Discard(ctx context.Context) error {
	req := &api.DiscardRequest{Snapshot: snap.id}
	resp, err := doPost[api.DiscardResponse](ctx, snap.db, "/snap/discard", req)
	if err != nil {
		return err
	}
	if len(resp.Error) != 0 {
		return string2error(resp.Error)
	}
	return nil
}

func doPost[RESP, REQ any](ctx context.Context, db *DB, subpath string, req *REQ) (*RESP, error) {
	u := url.URL{
		Host:   db.dbURL.Host,
		Scheme: db.dbURL.Scheme,
		Path:   path.Join(db.dbURL.Path, subpath),
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	r.Header.Set("content-type", "application/json")
	resp, err := db.httpClient.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Debug("kvhttp.Client", "url", u.String(), "request", req, "code", resp.StatusCode)
		// A 404 identifies an unknown transaction/snapshot/iterator name
		// (spec §7.1); surface it as os.ErrNotExist so errors.Is works.
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%s: %w", u.String(), os.ErrNotExist)
		}
		return nil, &httpStatusError{code: resp.StatusCode}
	}
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Debug("kvhttp.Client", "url", u.String(), "request", req, "code", resp.StatusCode, "err", err)
		return nil, err
	}
	response := new(RESP)
	if err := json.Unmarshal(respData, response); err != nil {
		return nil, err
	}
	slog.Debug("kvhttp.Client", "url", u.String(), "request", req, "response", response)
	return response, nil
}
