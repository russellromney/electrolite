# ROADMAP

Roadmap items are future work. This file captures the intended product
shape before implementation starts.

## North Star

Electrolite is a Rust-first embeddable SQLite sync layer that gives
browser clients Electric-style shape subscriptions without a separate sync
daemon.

```text
SQLite + generated triggers
  -> durable logical change log
  -> app-embedded shape endpoint
  -> browser client consumes snapshot + offset log
```

## Phase Spark - Tiny Honest MVP

Prove the semantics before optimizing.

### Scope

- Create Electrolite metadata tables:
  - `_electrolite_log`
  - `_electrolite_meta`
- Generate `AFTER INSERT`, `AFTER UPDATE`, and `AFTER DELETE` triggers
  for watched tables.
- Append logical changes with:
  - `seq`
  - `table_name`
  - `op`
  - `pk_json`
  - `old_json`
  - `new_json`
  - `created_at`
- Expose an embedded HTTP route from the host app:
  - `GET /electrolite/v1/shape/:name?offset=-1`
  - `GET /electrolite/v1/shape/:name?offset=123&live=true`
- Support named shapes only.
- Start with simple polling of `_electrolite_log` for live waits.

### First Commit Shape

- `electrolite-core` owns shape handles, predicates, log rows, and
  membership transition messages.
- `electrolite-sqlite` owns bootstrap DDL, trigger generation, and log
  reads.
- `electrolite-server` exists as a placeholder for the embedded HTTP
  route layer.

### Non-goals

- No arbitrary browser-provided SQL.
- No CDN/object-store chunking yet.
- No Honker or Walrust dependency yet.

## Phase Shape - Electric-ish Semantics

Make snapshot and replay behavior precise.

### Scope

- Initial snapshot high-water mark:
  1. read current max log `seq`
  2. run the shape query
  3. return rows plus `up-to-date` with the continuation offset
- Replay from offset by scanning `_electrolite_log`.
- Replay scans should filter by `table_name` before evaluating Shape
  membership.
- Keep a retained-log lower bound and return `409 resync_required` when
  the requested offset is older than retained history.
- Compact `_electrolite_log` with a durable retained-offset marker so
  resync still works even after all older rows are deleted.
- Evaluate membership transitions:

```text
old no,  new yes -> insert
old yes, new yes -> update
old yes, new no  -> delete
old no,  new no  -> ignore
```

- Define stable shape handles:

```text
hash(table + columns + where + params + auth_scope + schema_version)
```

- Return `409 resync_required` when the requested offset is older than
  retained history.
- Treat ordinary trigger rows as committed row-level changes. For
  app-controlled multi-row writes, support explicit Electrolite change
  batches so bounded replay does not split the batch.

## Phase Guard - Security Model

Make the default safe for real applications.

### Scope

- Require server-defined named shapes.
- Require column allowlists.
- Run authorization in host app code before serving a shape.
- Support TypeScript app servers as the authorization boundary by
  proxying browser Shape requests to an internal Electrolite origin with
  scoped headers.
- Keep sidecar deployment optional. The protocol is embedded-first; the
  TypeScript bridge exists so TypeScript apps can own authorization until
  native Node/Bun bindings exist.
- Include auth scope in shape handles.
- Keep raw `_electrolite_log` private.
- Add optional signed shape URLs for proxy/CDN/object-store delivery.
- Ensure delete messages only reveal rows previously visible to that
  authorized shape.

### Dynamic Authorization Gap

The server supports static named Shapes with an auth hook and dynamic
Shape factories for app routes such as:

```text
/users/:user_id/photos
/photos/:photo_id/friend-likes
```

Factories still build server-side Shapes; browsers still must not send
arbitrary SQL. A factory can reject a request before the Shape exists,
and the normal authorizer still checks the generated `auth_scope`.

Relationship-shaped data such as "your friends' likes on your photos"
still needs richer predicate/index support than the first `Eq`/`And`
predicate set.

## Phase Crowd - Fanout And Caching

Support 10,000 concurrent clients when most clients share a small number
of shape instances.

### Scope

- Materialize immutable response chunks by shape handle and offset.
- Make historical chunks cacheable:

```http
Cache-Control: public, max-age=31536000, immutable
ETag: ...
```

- Keep live delivery as HTTP long-polling, not WebSockets.
- Design live URLs so CDN request collapsing can coalesce identical
  `shape_handle + offset` waits.
- Origin should do one wait per `shape_handle + offset`, not one wait per
  browser.
- The embedded server should use a bounded SQLite connection pool with a
  default size of 1, configurable by the host app.
- Add a benchmark harness that measures snapshot latency, replay latency,
  live fanout latency, SQLite reads, and origin work across client counts.

### Target

- 10,000 concurrent clients.
- 10 to 100 active shared shape instances.
- Origin work scales with writes times affected shapes, not users.

## Phase Wake - Efficient Commit Notification

Remove dumb polling without changing semantics.

### Options

1. In-process condition variables when the host app owns writes.
2. SQLite update/preupdate hooks for controlled connections.
3. Honker integration for cross-process commit wakes.
4. Polling remains as the fallback.

Honker is an accelerant here, not the semantic core.

## Phase Index - Shape Evaluation Scale

Avoid evaluating every shape on every write.

### Scope

- Restrict fast-path predicates to:
  - `column = value`
  - `column IN (...)`
  - simple ranges later
- Maintain shape registry indexes:

```text
table=todos, project_id=p1 -> [shapeA, shapeB]
table=todos, done=false    -> [shapeA, shapeC]
```

- On each log row, find candidate shapes first, then evaluate membership
  exactly.
- Mark arbitrary predicates as slow path.

### Implemented Slice

- `ShapeIndex` indexes Shapes by table and equality predicate terms.
- `IN` predicates expand into equality terms for index candidate lookup.
- Candidate lookup considers both old and new row images so inserts,
  updates, deletes, and membership transitions are all eligible for exact
  evaluation.
- `Predicate::All` and predicates without equality terms remain table
  scan candidates.

## Phase Store - S3/Cinch Object Mode

Use object storage for immutable authorized shape chunks.

### Scope

- Store chunks under opaque shape handles.
- Keep raw logs private.
- Serve chunks through private proxy or signed URLs.
- Keep dynamic live waits in the app server or an edge worker.

## Phase Replica - Walrust Mode

Run shape servers near users on physical SQLite replicas.

```text
primary SQLite
  -> Electrolite trigger log
  -> Walrust physical replication
  -> replica SQLite
  -> Electrolite shape endpoint near users
```

Walrust provides physical replication, PITR, and remote read replicas.
Electrolite semantics still come from trigger logs.

## hadb Format Notes

hadb's `.hadbp` physical changesets are useful for moving SQLite pages to
replicas or object storage. They can help decide where an Electrolite
Shape endpoint runs, but they do not define Shape semantics.

hadb's planned `.hadbj` journal changesets are closer in spirit: a
logical journal with sequence, checksum chain, and deterministic replay.
Electrolite's trigger log is the local SQLite version of that semantic
journal. If Electrolite later exports chunks to S3/Cinch, borrowing the
hadb generation/key/checksum discipline would be more useful than trying
to infer Shapes from physical pages.

## Phase Later - Fancy Things

- WAL page invalidation to reduce shape evaluation work.
- Precomputed per-shape logs.
- Browser client adapters for React, Solid, Svelte, and vanilla stores.
- Native TypeScript backend ergonomics through Node/Bun bindings, so a
  TypeScript server can embed Electrolite without a separate process.
- IndexedDB persistence.
- Multi-tab coordination.
- Browser client retry/backoff/status reporting.
- Offline writes and conflict handling as a separate track.
