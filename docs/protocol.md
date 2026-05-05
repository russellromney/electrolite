# Protocol Sketch

The first protocol should be boring HTTP.

## Initial Sync

```http
GET /electrolite/v1/:shapeName/:params?offset=-1
```

The server:

1. authorizes the named shape for the current user
2. pins a SQLite read snapshot
3. records the current log high-water mark
4. queries current rows for the shape
5. returns rows with the continuation offset, key metadata, and shape handle

Example response:

```json
{
  "type": "snapshot",
  "log_id": "8f7c0f4c6c3b4d6f9b2a0d4e1f6a7b8c",
  "shape_handle": "36f5...",
  "key_columns": ["id"],
  "rows": [
    { "id": 7, "title": "ship electrolite", "done": false }
  ],
  "offset": 124,
  "up_to_date": true
}
```

The `offset` field is a continuation offset: the client sends it back to
ask for changes after that point.

There is an expected gap between the snapshot response and the next
`live=true` request. The snapshot offset closes that gap: any commit that
lands after the pinned snapshot has a higher log offset, so the first
replay/live request with the snapshot offset returns it.

## Replay

```http
GET /electrolite/v1/:shapeName/:params?offset=124
```

Example response:

```json
{
  "type": "replay",
  "log_id": "8f7c0f4c6c3b4d6f9b2a0d4e1f6a7b8c",
  "shape_handle": "36f5...",
  "messages": [
    {
      "type": "update",
      "batch_id": "batch-125",
      "key": { "id": 7 },
      "value": { "id": 7, "title": "ship electrolite", "done": true },
      "offset": 125
    }
  ],
  "offset": 125,
  "up_to_date": true
}
```

## Dynamic Shapes

Shapes are server-defined in the Node app and can read route params:

```http
GET /electrolite/v1/projectTodos/p1?offset=-1
```

For example, `projectTodos` can build a concrete Shape whose predicate is
`project_id = "p1"` and whose authorization scope is `project:p1`. The
browser still sends no SQL. The app's Shape definition sees the request
and route params, then can deny malformed or unauthorized route
parameters before SQLite is touched.

## Live Long-Poll

```http
GET /electrolite/v1/:shapeName/:params?offset=124&live=true&log_id=8f7c...
```

The server:

1. returns immediately if new messages exist
2. coalesces identical `shape_handle + offset` waits in-process
3. otherwise waits up to a bounded timeout
4. returns `204 No Content` if nothing changes
5. lets the client reconnect at the latest continuation offset

Long-polling keeps the endpoint HTTP/CDN friendly. WebSockets can come
later as an adapter, not the core protocol.

## Browser Materialization

The tiny browser client:

1. requests `offset=-1`
2. learns key columns from the snapshot and stores rows in a `Map`
3. drains replay pages until the server reports `up_to_date`
4. reconnects with `live=true`
5. applies insert, update, and delete messages
6. notifies subscribers after materialized rows change
7. reports connection/status changes
8. retries transient failures with backoff

The current snapshot response contains rows and `key_columns`, while
replay messages contain keys. The client can be configured with key
columns as an override, but the normal path is server-provided metadata.
Replay responses are staged and published to subscribers only after the
response reaches its `up_to_date` boundary. If a bounded replay page is
not up to date, the browser asks for the next replay page without
entering live long-poll mode.

Snapshots and replays include a `log_id`, a durable identity for the
SQLite/Electrolite log history. Clients persist `log_id` with cached rows
and send it with replay/live requests. If the database was reset,
restored, or swapped and the client's `log_id` no longer matches, the
server returns `409 resync_required`.

Snapshots and replays also include a `shape_handle`, a normalized identity
for the authorized Shape. Clients persist it with cached rows. If the app
changes the Shape definition behind the same URL, the next response has a
different handle and the client discards the stale cache.

Replay messages include `batch_id`. Messages written through an explicit
Electrolite write batch share a batch id, so UIs and tests can tell when
multiple row changes came from one backend batch. Ordinary SQLite writes
that do not use the Electrolite batch API are still valid, but their
replay contract is row-level.

If replay returns `409 resync_required`, the client clears materialized
rows, clears the cached continuation offset, and restarts from `offset=-1`.

Replay reads pin one SQLite snapshot for the retained-offset check, log
page read, and `up_to_date` decision. A bounded replay page reports
`up_to_date: false` when more table log rows remain after the returned
offset, so clients can stage partial pages until a consistency boundary.

Internally, replay is represented as a Shape replay page with a
`ShapeCursor`: the Shape handle, source log offset, retained source
offset, and source start/end offsets for the page. The public protocol
still exposes the familiar `offset` field, but the engine keeps the
Shape cursor metadata needed for retention, chunking, and fanout
work.

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

```ts
electrolite.writeBatch([
  ["UPDATE todos SET done = 1 WHERE project_id = ?1", ["p1"]],
]);
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

Electrolite can narrow fanout work before exact membership evaluation by
indexing Shapes by table and equality predicate terms, including each
value in an `IN` predicate. It still checks both old and new row images
for candidate matches, so rows entering and leaving a Shape stay visible
to the exact transition logic.

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

```ts
electrolite.compactLogToLastForTable("todos", 10_000);
```

After compaction, offsets older than the durable retained offset return
`409 resync_required` even if the compacted log table is empty. Global
compaction records per-table lower bounds, so unrelated table churn does
not force a quiet Shape to resync.

## Runtime Notes

Electrolite runs embedded in the Node app and uses Node's built-in
SQLite API. The project intentionally avoids a native build step for now.
Benchmarking snapshot, replay, and fanout behavior is future work.

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
