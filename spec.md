# kvhttp Protocol Specification

**Status:** Normative
**Package:** `github.com/visvasity/kvhttp`
**Depends on:** the `github.com/visvasity/kv` data model and interface contract.

This document specifies the wire protocol and API by which a
`kv`-compatible key-value database is exported over HTTP for remote access,
and the behavior a conforming client and server MUST provide. It is written
to describe the *intended* contract of the package, and where the current
implementation diverges from that intent, this specification is authoritative.

---

## 1. Conventions and Terminology

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**,
**SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this
document are to be interpreted as described in RFC 2119 and RFC 8174.

- **Server** — an HTTP endpoint that exports one underlying `kv.Database`.
- **Client** — a component that implements the `kv.Database` interface by
  issuing requests to a Server.
- **Object** — a server-side Transaction, Snapshot, or Iterator, each
  addressed by a client-assigned **name** (see §4).
- **Terminal outcome** — the final, irreversible state of a Transaction:
  either *committed* or *rolled back*.

### 1.1 Relationship to the `kv` contract

The Server exports an object implementing `kv.Database`; the Client presents
itself to callers as a `kv.Database`. This protocol MUST preserve the semantics
defined by the `kv` package, including:

- Keys are non-empty strings; the empty string is not a valid key and is
  reserved to denote range bounds (§3).
- Values are arbitrary byte sequences, modeled as `io.Reader`.
- Range iteration is lexicographical over the half-open interval `[begin, end)`
  (§3.3).
- The isolation level, conflict detection, and durability guarantees are those
  of the **underlying database**. This protocol neither strengthens nor weakens
  them; it transports them faithfully (§9).
- The `kv.Transaction.Commit` contract for remote databases (§8.9) is binding.

---

## 2. Transport and Encoding

### 2.1 HTTP

- Every operation is a single HTTP request/response exchange.
- Every request **MUST** use the `POST` method. A Server **MUST** reject other
  methods with HTTP `405 Method Not Allowed`.
- Every request **MUST** carry `Content-Type: application/json` (matched
  case-insensitively). A Server **MUST** reject other content types with HTTP
  `400 Bad Request`.
- All endpoints are addressed relative to a configurable **base path** (the
  path component of the Server's base URL). Endpoint paths in §8 are written
  relative to that base path. A base path of `/` yields the paths exactly as
  written.

### 2.2 Message encoding

- Request and response bodies are JSON objects.
- Fields typed as byte sequences (keys and values, e.g. `Key`, `Value`,
  `Begin`, `End`) are encoded as **base64** strings (RFC 4648 standard
  alphabet, with padding), consistent with Go's `encoding/json` treatment of
  `[]byte`.
- An absent, `null`, or empty byte field denotes the empty byte sequence.
- Unknown JSON fields in a request **SHOULD** be ignored by the Server to allow
  forward-compatible additions. Clients **SHOULD** ignore unknown fields in
  responses.
- An empty request body is permitted at the transport layer; endpoints that
  require fields **MUST** validate them and respond per §7.

---

## 3. Data Model

### 3.1 Keys

A key is a non-empty sequence of bytes. The empty key is invalid as an operand
to `Get`, `Set`, or `Delete`; a Server **MUST** treat an empty operand key as
`ErrInvalid` (§7).

### 3.2 Values

A value is an arbitrary (possibly empty) sequence of bytes. A `Set` with a
`nil`/absent value source is invalid (`ErrInvalid`); a `Set` with an empty
value is valid and stores an empty value.

### 3.3 Ranges

Range operations (`Ascend`, `Descend`) iterate lexicographically over the
half-open interval `[begin, end)`:

- `begin == ""` → no lower bound (start at the smallest key).
- `end == ""` → no upper bound (continue through the largest key).
- `begin == "" && end == ""` → the entire keyspace.
- If both bounds are non-empty and `begin > end` (byte-wise), the range is
  invalid: the iterator **MUST** yield no items and the operation **MUST**
  surface `ErrInvalid` (§8.11).

`Ascend` yields keys in increasing order; `Descend` yields keys in decreasing
order. Both observe the same `[begin, end)` bounds.

---

## 4. Object Model and Naming

Transactions, Snapshots, and Iterators are stateful, server-side objects.

- **Clients assign names.** Each object is created with a client-chosen name
  carried in the request. Names **MUST** be unique per Server across all live
  objects and **SHOULD** be universally unique (e.g. a UUID). The Client uses
  the name to address the object in all subsequent requests.
- The Server maps each name to internal state. The name is the Client's
  **idempotency key** for that object (§8.9, §10).
- Names are opaque to the protocol; the Server **MUST NOT** attach meaning to
  their contents beyond equality.
- **Name lifetime.** A name is *live* from successful creation until its object
  reaches a terminal state (transaction committed/rolled back, snapshot
  discarded, iterator exhausted/closed). After that the name is *retired*; the
  Server retains the terminal outcome for a retention window (§10) so that
  retried requests can learn it.

A Server **MUST** serialize concurrent requests that address the same name, so
that operations on a single object do not interleave in a way that corrupts its
state. Requests addressing distinct names **MAY** proceed concurrently.

---

## 5. Object Ownership of Iterators

An Iterator is created against a parent Transaction or Snapshot and reads from
that parent's view. A Transaction or Snapshot **MAY** have multiple live
Iterators simultaneously.

When a parent reaches its terminal state (transaction commit/rollback, snapshot
discard), the Server **MUST** close and retire all Iterators owned by that
parent. Subsequent `Next` requests on those iterators **MUST** report
`ErrClosed` (§7) if the iterator name is still within its retention window, or
`ErrNotExist` (HTTP `404`) once retired beyond it.

---

## 6. Reader/Writer Roles

- A **Transaction** is a read-write object: it supports `Get`, `Set`,
  `Delete`, `Ascend`, `Descend`, `Commit`, and `Rollback`.
- A **Snapshot** is a read-only object providing a consistent, repeatable-read
  view: it supports `Get`, `Ascend`, `Descend`, and `Discard`.
- The `Get`, `Ascend`, and `Descend` requests are shared shapes usable against
  either a Transaction or a Snapshot; the request identifies the parent by
  setting exactly one of the `Transaction` or `Snapshot` fields (§8.4, §8.11).

---

## 7. Error Model

Errors are reported over **two distinct channels**. A Client MUST handle both.

### 7.1 Transport-level errors (non-200 HTTP status)

These indicate the request could not be dispatched to the underlying database
operation, or is malformed at the protocol level. The response body is
human-readable text (not the JSON response schema). Defined statuses:

| Status | Meaning |
|--------|---------|
| `400 Bad Request` | Wrong content type, unparseable body, or invalid operands (e.g. neither/both of `Transaction`/`Snapshot` set). |
| `404 Not Found` | The referenced name is unknown to the Server (never existed, or retired beyond its retention window). |
| `405 Method Not Allowed` | Non-`POST` method. |
| `409 Conflict` | A create request used a name that is already live (§8.1, §8.10, §8.11). |
| `500 Internal Server Error` | Unexpected Server-side failure. |

A Client **MUST NOT** interpret a transport-level error as the JSON response
schema.

### 7.2 Application-level errors (HTTP 200 with `Error` field)

Every response schema contains an `Error` string field. A successful operation
returns HTTP `200` with `Error` empty (`""`). A database-level failure of an
otherwise well-formed operation returns HTTP `200` with `Error` non-empty.

The `Error` field carries a **stable sentinel token** for the well-known
errors of the `kv` contract, so that `errors.Is` comparisons survive the round
trip:

| Token | Reconstructed error |
|-------|---------------------|
| `ErrClosed` | `os.ErrClosed` |
| `ErrInvalid` | `os.ErrInvalid` |
| `ErrNotExist` | `os.ErrNotExist` |
| `EOF` | `io.EOF` |

Any other non-empty `Error` value is an opaque error message; the Client
reconstructs it as an error carrying that message text. A Client **MUST**
recognize the tokens above and map them back to the corresponding sentinel
errors.

### 7.3 Channel selection (normative)

- **Malformed or misrouted requests** (bad method, content type, body, invalid
  operand combination) → transport-level error (§7.1).
- **Name resolution** — an unknown name → HTTP `404`; a name whose object has
  reached a terminal state but is still within its retention window →
  HTTP `200` with `Error: "ErrClosed"`. This distinction lets a Client
  distinguish "definitely gone/forgotten" from "terminated, outcome known."
- **Create-time name collision** with a live name that is *not* a retry of the
  same object → HTTP `409` (but see §8.1/§8.10/§8.11 for the idempotent-retry
  case, which returns success).
- **Database operation results** (key not found, commit conflict, operation on
  a closed object, invalid key/range) → HTTP `200` with the appropriate
  `Error` token or message.

---

## 8. Operations

Each operation below lists its endpoint, request fields, response fields,
semantics, and idempotency. All requests are `POST` with a JSON body per §2.
All responses include the `Error` field (§7.2), omitted from the per-operation
field lists for brevity.

### 8.1 Create Transaction — `POST {base}/new-transaction`

**Request:** `{ "Name": string }` — the client-assigned transaction name.

**Response:** `{ "Error": string }`

**Semantics.** Creates a new read-write transaction on the underlying database
and binds it to `Name`. On success the transaction is live and addressable by
`Name`.

**Idempotency.** The operation is idempotent with respect to `Name`. If the
Client retries creation with the same `Name` and the transaction is still live
(created by an earlier attempt whose response may have been lost), the Server
**SHOULD** return success rather than `409 Conflict`. A `409 Conflict` is
reserved for a genuine collision with an unrelated live object; because names
are client-assigned unique identifiers, a Client **MAY** treat a `409` on its
own freshly generated name as evidence the prior attempt succeeded.

### 8.2 Create Snapshot — `POST {base}/new-snapshot`

**Request:** `{ "Name": string }`

**Response:** `{ "Error": string }`

**Semantics.** Creates a read-only snapshot providing a consistent,
repeatable-read view, bound to `Name`. The view is established at creation or on
first read, per the underlying database.

**Idempotency.** As §8.1, with respect to `Name`.

### 8.3 Transaction Get — `POST {base}/tx/get`

**Request:** `{ "Transaction": string, "Key": bytes }`

**Response:** `{ "Error": string, "Value": bytes }`

**Semantics.** Reads `Key` within the named transaction's view (reflecting the
transaction's own uncommitted writes). On success returns `Value`. A missing
key **MUST** yield `Error: "ErrNotExist"`; an empty key **MUST** yield
`Error: "ErrInvalid"`.

**Idempotency.** Read-only; safe to retry.

### 8.4 Snapshot Get — `POST {base}/snap/get`

**Request:** `{ "Snapshot": string, "Key": bytes }`

**Response:** `{ "Error": string, "Value": bytes }`

**Semantics.** As §8.3, reading within the named snapshot's view.

**Idempotency.** Read-only; safe to retry.

> Note: The `Get` request schema carries both `Transaction` and `Snapshot`
> fields; the `/tx/get` and `/snap/get` endpoints select which is meaningful.
> Exactly one **MUST** be set (§7.3).

### 8.5 Set — `POST {base}/tx/set`

**Request:** `{ "Transaction": string, "Key": bytes, "Value": bytes }`

**Response:** `{ "Error": string }`

**Semantics.** Buffers a write of `Key`→`Value` in the named transaction.
An empty `Key` **MUST** yield `ErrInvalid`. An absent value source is invalid
(`ErrInvalid`); an empty value is valid. The write is not durable until the
transaction commits (§8.9).

**Idempotency.** Safe to retry while the transaction is live: replaying the
same `Set` re-establishes the same buffered write.

### 8.6 Delete — `POST {base}/tx/delete`

**Request:** `{ "Transaction": string, "Key": bytes }`

**Response:** `{ "Error": string }`

**Semantics.** Buffers a delete of `Key` in the named transaction. An empty
`Key` **MUST** yield `ErrInvalid`.

**Idempotency.** Safe to retry while the transaction is live.

> The path `{base}/tx/del` is accepted as a deprecated alias for
> `{base}/tx/delete`. New clients **MUST** use `{base}/tx/delete`.

### 8.7 Transaction Rollback — `POST {base}/tx/rollback`

**Request:** `{ "Transaction": string }`

**Response:** `{ "Error": string }`

**Semantics.** Cancels the transaction without conflict checking, discarding
all buffered writes, and drives the transaction to the *rolled back* terminal
state. All Iterators owned by the transaction are closed (§5). Rollback of an
already-rolled-back transaction returns success (`Error: ""`). Rollback of an
already-*committed* transaction returns `Error: "ErrClosed"`.

**Idempotency.** Idempotent with respect to `Transaction`: retries after the
terminal state is reached return the recorded outcome (§8.9, §10).

### 8.8 Snapshot Discard — `POST {base}/snap/discard`

**Request:** `{ "Snapshot": string }`

**Response:** `{ "Error": string }`

**Semantics.** Releases the snapshot and closes all Iterators it owns (§5),
driving it to a terminal state. Discard of an already-discarded snapshot within
its retention window returns success.

**Idempotency.** Idempotent with respect to `Snapshot`.

### 8.9 Transaction Commit — `POST {base}/tx/commit`

**Request:** `{ "Transaction": string }`

**Response:** `{ "Error": string }`

**Semantics.** Validates the transaction's reads and writes for conflicts and,
if valid, atomically applies its writes to the database, driving it to the
*committed* terminal state. On success returns `Error: ""`. A conflict or other
commit failure returns a non-empty `Error` (an opaque message, or a sentinel if
applicable) and drives the transaction to the *rolled back* terminal state.
All Iterators owned by the transaction are closed (§5).

**Idempotency and retry-to-confirm (REQUIRED).** This operation implements the
remote-commit contract of `kv.Transaction.Commit`:

1. A Server **MUST** durably record each transaction name's terminal outcome
   (committed vs rolled back) and retain it for a retention window (§10).
2. Commit of an already-*committed* name **MUST** return success
   (`Error: ""`). Commit **MUST NOT** report failure for a transaction that was
   in fact committed — including the case where an earlier commit succeeded but
   its response was lost.
3. Commit of an already-*rolled-back* name **MUST** return a non-empty `Error`
   (e.g. `ErrClosed`), reporting the definite non-committed outcome.
4. Because a lost response leaves the outcome *in doubt* at the Client, a
   Client **SHOULD** retry commit (subject to its own deadline/backoff policy)
   until it obtains a definite outcome per (2) or (3). Retrying commit is safe:
   the Server never re-executes a committed transaction and never double-applies
   its writes.
5. If the transaction name has been retired beyond the retention window, the
   Server returns HTTP `404` and the outcome is genuinely unrecoverable through
   this protocol; §10 governs how long a Server retains outcomes to avoid this.

### 8.10 Ascend — `POST {base}/tx/ascend` or `POST {base}/snap/ascend`

**Request:**
`{ "Transaction": string, "Snapshot": string, "Begin": bytes, "End": bytes, "Name": string }`

Exactly one of `Transaction` / `Snapshot` **MUST** be set, matching the
endpoint. `Name` is the client-assigned **iterator** name.

**Response:** `{ "Error": string }`

**Semantics.** Creates a server-side iterator positioned to yield keys in
ascending order over `[Begin, End)` (§3.3) within the parent's view, and binds
it to `Name`. The iterator is owned by the parent (§5). Creation does not
return data; the Client reads via `Next` (§8.13). An invalid range (§3.3) is
surfaced through the iterator: creation **MAY** succeed and the first `Next`
report `ErrInvalid`, or creation itself **MAY** report it; a Client **MUST** be
prepared for either.

**Idempotency.** Idempotent with respect to `Name`. Because a freshly created
iterator has not yet advanced, a retried create with the same `Name`
**SHOULD** return success (the iterator is positioned at its start).

### 8.11 Descend — `POST {base}/tx/descend` or `POST {base}/snap/descend`

As §8.10, but the iterator yields keys in **descending** order over
`[Begin, End)`.

### 8.12 (Reserved)

Reserved. Earlier drafts of the `api` package declared `Scan` request/response
types. `Scan` is **not** part of the `kv` interface and is **not** defined by
this specification; the reserved wire types carry no normative meaning and
conforming implementations **MUST NOT** rely on them.

### 8.13 Iterator Next — `POST {base}/it/next`

**Request:** `{ "Iterator": string }`

**Response:** `{ "Error": string, "Key": bytes, "Value": bytes }`

**Semantics.** Advances the named iterator by one entry and returns its `Key`
and `Value`. Iteration terminates as follows:

- **End of iteration (normal):** the Server returns `Error: ""` with an empty
  `Key`. An empty `Key` in a non-error response is the sole end-of-iteration
  signal (keys are never empty, §3.1). After this the iterator is retired.
- **Error during iteration:** the Server returns a non-empty `Error` (e.g.
  `ErrInvalid` for an invalid range, or the underlying error). Iteration stops
  at the first error.

After the parent transaction/snapshot terminates, `Next` on an owned iterator
reports `ErrClosed` (within retention) or HTTP `404` (once retired), per §5.

**Idempotency (NOT idempotent).** `Next` mutates iterator position and is **not**
safe to blindly retry: a retried `Next` after a lost response would **skip** an
entry. If a `Next` response is lost, a Client **MUST NOT** replay the same
`Next`. Instead, the Client **SHOULD** resume by creating a **new** iterator
over the remaining range, relying on the parent's stable view: for `Ascend`,
set `Begin` to the last successfully received key (and skip that key if
re-yielded, since the range is inclusive of `Begin`); for `Descend`, set `End`
to the last successfully received key. Because a Transaction or Snapshot
provides a repeatable-read view, re-iteration yields a consistent sequence.

### 8.14 Debug — `POST {base}/debug` (OPTIONAL, non-normative)

A Server **MAY** expose a diagnostic endpoint for operational introspection.
Its presence, request/response shape, and behavior are implementation-defined
and **MUST NOT** be relied upon by conforming Clients.

---

## 9. Isolation, Concurrency, and Conflicts

- The isolation level and conflict-detection policy are those of the
  **underlying database**; this protocol transports them without alteration.
- A Server **MUST** serialize concurrent requests addressing the same object
  name (§4) so that per-object state is not corrupted. Requests against
  distinct names **MAY** run concurrently.
- Two concurrent transactions that conflict (per the underlying database's
  rules — e.g. read-write or write-write conflicts) **MUST** produce at most
  one successful commit for the conflicting set; the other commit(s) return a
  non-empty `Error`. Non-conflicting concurrent transactions (e.g. independent
  blind writes to different keys) **MAY** all commit successfully. The exact
  determination of "conflict" is defined by the underlying database, not by
  this specification.
- A committed transaction's writes become visible to views established after
  the commit, per the underlying database's visibility rules.

---

## 10. Resource Lifecycle and Reclamation

Server-side objects (transactions, snapshots, iterators) and retained terminal
outcomes consume resources.

- A Client **SHOULD** promptly drive every object to its terminal state
  (commit/rollback a transaction, discard a snapshot, exhaust or abandon an
  iterator) rather than relying on Server reclamation.
- A Server **SHOULD** reclaim objects that are abandoned (e.g. idle beyond an
  implementation-defined period, or orphaned by a lost Client). After
  reclamation, requests against the reclaimed name behave as for a terminated
  object: `ErrClosed` while the terminal outcome is retained, then HTTP `404`
  once retired.
- A Server **SHOULD** retain each transaction's terminal outcome long enough to
  satisfy the retry-to-confirm contract (§8.9) for any Client that may still be
  retrying. Reclaiming an outcome too early reintroduces an unrecoverable
  in-doubt state (§8.9 item 5); implementations **SHOULD** choose a retention
  window that dominates expected Client retry horizons.
- The exact reclamation policy, idle timeouts, and retention window are
  **implementation-defined** and outside the scope of this specification. A
  Server **MUST NOT**, however, reclaim a live (non-terminal) object that is
  still being actively used within the implementation's declared idle bounds.

---

## 11. Client Requirements Summary

A conforming Client:

- **MUST** send well-formed `POST`/JSON requests per §2 and address objects by
  the names it assigns (§4).
- **MUST** handle both error channels (§7): interpret non-200 statuses as
  transport errors and non-empty `Error` fields as application errors, mapping
  sentinel tokens back to `os.ErrClosed`, `os.ErrInvalid`, `os.ErrNotExist`,
  and `io.EOF`.
- **MUST** treat an empty `Key` in a successful `Next` response as
  end-of-iteration (§8.13).
- **SHOULD** retry `Commit` until it obtains a definite committed/rolled-back
  outcome, and **MUST NOT** treat a lost/timed-out commit response as failure
  (§8.9).
- **MUST NOT** blindly replay `Next`; it **SHOULD** resume via a fresh iterator
  (§8.13).
- **SHOULD** use universally unique object names to make creation and commit
  safely retryable (§4, §8.1, §8.9).

## 12. Server Requirements Summary

A conforming Server:

- **MUST** enforce the transport rules of §2 and the channel-selection rules of
  §7.3.
- **MUST** serialize per-name access and **MAY** parallelize across names (§9).
- **MUST** honor the commit idempotency and terminal-outcome retention rules of
  §8.9 and §10.
- **MUST** close a parent's iterators when the parent terminates (§5).
- **MUST** preserve, and **MUST NOT** alter, the underlying database's
  isolation and conflict semantics (§9).

---

## 13. Out of Scope

The following are intentionally **out of scope** for this specification and are
deferred to the deployment and transport layers:

- **Authentication and authorization.** This protocol defines no credentials,
  identity, or access-control model. Deployments requiring them **SHOULD** layer
  them at the transport/gateway (e.g. mTLS, bearer tokens, a reverse proxy).
- **Transport security.** Confidentiality and integrity (e.g. TLS) are a
  deployment concern; this protocol does not mandate them but deployments over
  untrusted networks **SHOULD** use TLS.
- **Protocol versioning and negotiation.** This document specifies a single
  version of the protocol. Versioning, capability negotiation, and
  compatibility signaling are not defined here and **MAY** be added by a future
  revision.
- **Rate limiting, quotas, and multi-tenancy.** These are deployment concerns.

---

## Appendix A. Endpoint Index

| Endpoint | Op | Idempotent | §  |
|----------|----|-----------|----|
| `POST {base}/new-transaction` | Create transaction | Yes (by name) | 8.1 |
| `POST {base}/new-snapshot` | Create snapshot | Yes (by name) | 8.2 |
| `POST {base}/tx/get` | Transaction read | Yes | 8.3 |
| `POST {base}/snap/get` | Snapshot read | Yes | 8.4 |
| `POST {base}/tx/set` | Buffered write | Yes (while live) | 8.5 |
| `POST {base}/tx/delete` (`/tx/del` alias) | Buffered delete | Yes (while live) | 8.6 |
| `POST {base}/tx/rollback` | Rollback | Yes | 8.7 |
| `POST {base}/snap/discard` | Discard snapshot | Yes | 8.8 |
| `POST {base}/tx/commit` | Commit | Yes (retry-to-confirm) | 8.9 |
| `POST {base}/tx/ascend` | Create asc. iterator | Yes (by name) | 8.10 |
| `POST {base}/snap/ascend` | Create asc. iterator | Yes (by name) | 8.10 |
| `POST {base}/tx/descend` | Create desc. iterator | Yes (by name) | 8.11 |
| `POST {base}/snap/descend` | Create desc. iterator | Yes (by name) | 8.11 |
| `POST {base}/it/next` | Advance iterator | **No** | 8.13 |
| `POST {base}/debug` | Diagnostics (optional) | — | 8.14 |

## Appendix B. Error Token Reference

| `Error` value | Meaning | Reconstructed as |
|---------------|---------|------------------|
| `""` (empty) | Success | (no error) |
| `ErrClosed` | Object reached terminal state | `os.ErrClosed` |
| `ErrInvalid` | Invalid key, value, or range | `os.ErrInvalid` |
| `ErrNotExist` | Key not found | `os.ErrNotExist` |
| `EOF` | End of stream | `io.EOF` |
| any other text | Opaque database error | `errors.New(text)` |
