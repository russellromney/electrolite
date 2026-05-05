# Protocol Sketch

The first protocol should be boring HTTP.

## Initial Sync

```http
GET /electrolite/v1/shape/:name?offset=-1
```

The server:

1. authorizes the named shape for the current user
2. pins a SQLite read snapshot
3. records the current log high-water mark
4. queries current rows for the shape
5. returns rows with the continuation offset

Example response:

```json
{
  "type": "snapshot",
  "key_columns": ["id"],
  "rows": [
    { "id": 7, "title": "ship electrolite", "done": false }
  ],
  "offset": 124,
  "up_to_date": true
}
```

There is an expected gap between the snapshot response and the next
`live=true` request. The snapshot offset closes that gap: any commit that
lands after the pinned snapshot has a higher log offset, so the first
replay/live request with the snapshot offset returns it.

## Replay

```http
GET /electrolite/v1/shape/:name?offset=124
```

Example response:

```json
{
  "type": "replay",
  "messages": [
    {
      "type": "update",
      "key": { "id": 7 },
      "value": { "id": 7, "title": "ship electrolite", "done": true },
      "offset": 125
    }
  ],
  "offset": 125,
  "up_to_date": true
}
```

## Dynamic Shape Factories

Static Shapes use:

```http
GET /electrolite/v1/shape/:name?offset=-1
```

Dynamic Shapes use a server-registered factory:

```http
GET /electrolite/v1/factory/:factory/:path?offset=-1
```

For example, `/electrolite/v1/factory/projectTodos/p1?offset=-1`
can build a concrete Shape whose predicate is `project_id = "p1"` and
whose authorization scope is `project:p1`. The browser still sends no
SQL. The factory sees request headers/extensions and can deny malformed
or unauthorized route parameters before a Shape is served; then the
normal authorizer checks the generated Shape.

## Live Long-Poll

```http
GET /electrolite/v1/shape/:name?offset=124&live=true
```

The server:

1. returns immediately if new messages exist
2. coalesces identical `shape_handle + offset` waits in-process
3. otherwise waits up to a bounded timeout
4. returns `204 No Content` if nothing changes
5. lets the client reconnect at the latest offset

Long-polling keeps the endpoint HTTP/CDN friendly. WebSockets can come
later as an adapter, not the core protocol.

## Browser Materialization

The tiny browser client:

1. requests `offset=-1`
2. learns key columns from the snapshot and stores rows in a `Map`
3. reconnects with `live=true`
4. applies insert, update, and delete messages
5. notifies subscribers after materialized rows change
6. reports connection/status changes
7. retries transient failures with backoff

The current snapshot response contains rows and `key_columns`, while
replay messages contain keys. The client can be configured with key
columns as an override, but the normal path is server-provided metadata.
Replay responses are staged and published to subscribers only after the
response reaches its `up_to_date` boundary.

If replay returns `409 resync_required`, the client clears materialized
rows and restarts from `offset=-1`.

## Membership Transitions

For each log row:

```text
old matches? new matches?

false -> true   insert
true  -> true   update
true  -> false  delete
false -> false  ignore
```

## Change Batches

SQLite triggers only expose committed rows: rollback removes both the app
write and the Electrolite log rows. Raw writes are therefore safe, but
they are row-level for replay purposes and do not promise transaction
batch boundaries.

For app-controlled multi-row writes that should not be split by bounded
replay, hosts can use Electrolite change batches. Rows written inside a
batch share a logical batch ID; replay may exceed the configured row
limit to include the rest of the final batch.

```rust
state.write_batch(|tx| {
  tx.execute("UPDATE todos SET done = 1 WHERE project_id = 'p1'", [])?;
  Ok(())
}).await?;
```

This is the canonical write path when transaction-like replay boundaries
matter. Ordinary SQLite writes remain supported, but their semantic
contract is row-level.

## Type Policy

Electrolite inspects SQLite declared column types for watched tables and
normalizes logged JSON values and predicate values through the same
policy. Boolean JSON predicates are accepted for boolean-ish declared
columns such as `BOOLEAN`, where they normalize to integer `1`/`0`.
Boolean predicates against plain `INTEGER` columns are rejected so the
snapshot SQL and replay JSON matcher cannot silently disagree.

Blob columns are not supported in Shapes.

## Predicate Index

The core crate has a `ShapeIndex` that narrows fanout work before exact
membership evaluation. It indexes by table and equality predicate terms,
including each value in an `IN` predicate, then checks both old and new
row images for candidate matches. That keeps rows entering and leaving a
Shape visible to the exact transition logic.

## Resync

If a client asks for an offset older than retained history for that
table:

```http
409 Conflict
```

```json
{ "error": "resync_required" }
```

The client restarts with `offset=-1`.

Hosts can compact the log while preserving that lower bound:

```rust
state.compact_log_to_last(10_000).await?;
```

After compaction, offsets older than the durable retained offset return
`409 resync_required` even if the compacted log table is empty. Global
compaction records per-table lower bounds, so unrelated table churn does
not force a quiet Shape to resync.

For table-local compaction:

```rust
state.compact_log_to_last_for_table("todos", 10_000).await?;
```

## Runtime Notes

The embedded server uses a bounded SQLite connection pool. The default
pool size is 1, which is the conservative SQLite-friendly setting for
small apps. Hosts can raise the pool size when read concurrency matters.

```rust
let state = ServerState::new(db_path, registry, AppAuthorizer)
  .with_connection_pool_size(4);
```

The basic fanout benchmark can be run with:

```sh
cargo run -p electrolite-server --example fanout
```

Useful knobs:

```sh
ELECTROLITE_BENCH_ROWS=10000 \
ELECTROLITE_BENCH_CLIENTS=1000 \
ELECTROLITE_BENCH_POOL=8 \
cargo run -p electrolite-server --example fanout
```

## Public Errors

The HTTP server returns stable public error bodies. Internal SQLite and
Electrolite details are not serialized to clients.

```json
{ "error": "internal_server_error" }
```

Denied Shapes return the same public response as missing Shapes:

```json
{ "error": "shape_not_found" }
```
