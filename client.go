// Copyright (c) 2024 Visvasity LLC

package kvhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/google/uuid"

	"github.com/visvasity/kvhttp/api"
)

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
	resp, err := doPost[api.CommitResponse](ctx, tx.db, "/tx/commit", req)
	if err != nil {
		return err
	}
	if len(resp.Error) != 0 {
		return string2error(resp.Error)
	}
	return nil
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
		return nil, fmt.Errorf("received non-ok http status %d", resp.StatusCode)
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
