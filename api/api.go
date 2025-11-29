// Copyright (c) 2024 Visvasity LLC

package api

type NewTransactionRequest struct {
	Name string
}

type NewTransactionResponse struct {
	Error string
}

type NewSnapshotRequest struct {
	Name string
}

type NewSnapshotResponse struct {
	Error string
}

type GetRequest struct {
	Transaction string
	Snapshot    string

	Key []byte
}

type GetResponse struct {
	Error string

	Value []byte
}

type SetRequest struct {
	Transaction string

	Key []byte

	Value []byte
}

type SetResponse struct {
	Error string
}

type DeleteRequest struct {
	Transaction string

	Key []byte
}

type DeleteResponse struct {
	Error string
}

type AscendRequest struct {
	Transaction string
	Snapshot    string

	Begin []byte

	End []byte

	Name string
}

type AscendResponse struct {
	Error string
}

type DescendRequest struct {
	Transaction string
	Snapshot    string

	Begin []byte

	End []byte

	Name string
}

type DescendResponse struct {
	Error string
}

type ScanRequest struct {
	Transaction string
	Snapshot    string

	Name string
}

type ScanResponse struct {
	Error string
}

type NextRequest struct {
	Iterator string
}

type NextResponse struct {
	Error string

	Key []byte

	Value []byte
}

type CommitRequest struct {
	Transaction string
}

type CommitResponse struct {
	Error string
}

type RollbackRequest struct {
	Transaction string
}

type RollbackResponse struct {
	Error string
}

type DiscardRequest struct {
	Snapshot string
}

type DiscardResponse struct {
	Error string
}
