# Electrolite engine protocol

This is the contract every Electrolite engine must satisfy. It is the
behavior the browser client and any other Electrolite client depend on,
regardless of language. New engines are correct only if they pass each
case here.

The Node engine is the canonical reference; this doc is the user-facing
summary of what that engine guarantees. The Python, Rust, Go, and Elixir
engines all implement this contract.

## Wire types

### `snapshot` response

```json
{
  "type": "snapshot",
  "log_id": "<32 hex chars>",
  "shape_handle": "<64 hex chars, sha256 of normalized shape>",
  "key_columns": ["..."],
  "rows": [ { ... } ],
  "offset": <int>,
  "up_to_date": true
}
```

- Rows are ordered by primary-key columns.
- Rows are filtered by the shape predicate.
- Each row contains only the columns listed in the shape.
- `offset` pins the snapshot to a log position; subsequent replays use it.

### `replay` response

```json
{
  "type": "replay",
  "log_id": "<...>",
  "shape_handle": "<...>",
  "messages": [
    { "type": "insert", "batch_id": "...", "key": {...}, "offset": <int>, "value": {...} },
    { "type": "update", "batch_id": "...", "key": {...}, "offset": <int>, "value": {...} },
    { "type": "delete", "batch_id": "...", "key": {...}, "offset": <int> }
  ],
  "offset": <int>,
  "up_to_date": <bool>
}
```

- A primary-key change generates `delete` of the old key followed by
  `insert` of the new key.
- A row that previously matched the predicate but no longer does
  generates a `delete`.
- A row that did not match but now does generates an `insert`.
- A row that matched before and after with the same key generates an
  `update`.
- Replay tries to never return a partial batch. If a page would
  split a `batch_id` group, the engine extends the page to include
  the rest of that batch — up to a safety cap of `10 ×
  replay_limit` extra rows. Batches larger than the cap are split
  across replays; the response sets `up_to_date: false` so the
  client knows to fetch again immediately. Use a larger
  `replay_limit` if your application produces known-large batches.
- `value` is omitted on `delete`.

### Error responses

- `404 { "error": "shape_not_found" }` — unknown shape name *or*
  `authorize` returned false. The two are deliberately indistinguishable.
- `409 { "error": "resync_required" }` — one of:
  - client's `offset` is below the table's `retained_offset`
  - client's `log_id` does not match the current log id
  - client's `shape_handle` does not match the shape's current handle

## Shape definition

A shape is `(table, columns, predicate, auth_scope, schema_version)`.
Engines may add ergonomic API for `params`, `where`, `authorize`, and
`scope` callbacks; the resulting shape after callbacks must reduce to
the five core fields.

## Predicates

Required types:

- `{ type: "all" }`
- `{ type: "eq", column, value }`
- `{ type: "gt"|"lt"|"gte"|"lte", column, value }` — range, value must
  not be null.
- `{ type: "in", column, values }` — `null` allowed; engines must emit
  `IS NULL` for null entries.
- `{ type: "and", predicates: [...] }` — children combined with `AND`.

Each predicate type must:

- Compile to SQL for snapshot.
- Evaluate against a JSON row dict for replay-time filtering.
- Normalize deterministically so the shape handle is identical across
  engines (sort `in` values, sort `and` children).

### Numeric edge cases in `in` dedup

Engines dedup `in` values by canonical JSON encoding. JSON does not
distinguish integers from same-valued floats: `1` and `1.0` may
encode the same way (`"1"`) or differently (`"1"` vs `"1.0"`)
depending on the language. Today every engine produces the same
encoding for integers passed in, but if you pass a float that is
exactly an integer value (e.g. `1.0` from JS), the engine you happen
to be running may either dedup it against `1` or treat it as
distinct. Avoid mixing `1` and `1.0` in the same `in` list.

### Bad-input boundary

Predicates that reference a column that does not exist, or that
attach a boolean value to a non-`BOOLEAN`-affinity column, or that
use `null` with a range op, surface as `400 bad_request` from
`handle()`. Direct `snapshot()` / `replay()` callers receive a
language-native error (`Error::BadInput` in Rust, `errBadInput` in
Go, `{:bad_input, msg}` in Elixir, `BadInput` exception in Python,
an `Error` with `electroliteBadInput = true` in Node).

## Shape handle

```
shape_handle = sha256(canonical_json(normalized_shape))
```

Where `normalized_shape` is:

```json
{
  "auth_scope": "...",
  "columns": [/* sorted */],
  "predicate": /* normalized */,
  "schema_version": <int>,
  "table": "..."
}
```

`canonical_json` writes object keys in sorted order with no whitespace.
The 32-byte digest is hex-lowercased.

The same shape definition must produce identical `shape_handle` bytes in
every engine.

## SQLite tables

Each engine creates these on bootstrap:

- `_electrolite_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`
  - `log_id` — 32-hex random, generated once on first bootstrap.
  - `current_batch_id` — present only inside a `write_batch`.
  - `retained_offset:<table>` — set by `compact`.
- `_electrolite_watched_tables(table_name TEXT PRIMARY KEY, pk_columns TEXT)`
- `_electrolite_log(seq, batch_id, table_name, op, pk_json, old_pk_json,
  new_pk_json, old_json, new_json, created_at)`
- Index `_electrolite_log_table_seq_idx` on `(table_name, seq)`.

## Triggers

`install_triggers(table)` creates AFTER INSERT/UPDATE/DELETE triggers
on `table` that write one row per change to `_electrolite_log` with:

- `batch_id` = `COALESCE(meta.current_batch_id, lower(hex(randomblob(16))))`.
- `op` = `'insert'|'update'|'delete'`.
- `pk_json`, `old_pk_json`, `new_pk_json`, `old_json`, `new_json` as
  appropriate.

`install_triggers` requires the table to have a primary key.

## Write batches

`write_batch(statements)` runs all statements inside a single SQLite
transaction with a fresh `current_batch_id` set in meta for the
duration. Every log row produced inside the transaction shares the same
`batch_id`. On rollback, no log rows are visible.

## Live wait

When a client requests `live=true` with a non-negative offset, the
engine:

1. Replays once. If the replay produces messages, return them.
2. Otherwise, if `up_to_date` is true, block until a change is
   committed *or* `live_timeout_ms` elapses.
3. Replay one more time and return that.

Implementations may use a Condvar / channel / process mailbox / SQLite
`update_hook` to drive the wake. The user-visible behavior must match.

## Transports

Two transports are supported. Both speak the same wire types above.

### Long-poll (default)

Client `GET`s the shape URL with `Accept: application/json`. Server
responds when there is data or when `live_timeout_ms` elapses.

### Server-Sent Events

Client `GET`s the shape URL with `Accept: text/event-stream`. Server
responds with `Content-Type: text/event-stream` and writes events:

```text
event: snapshot
data: {snapshot body}

event: replay
data: {replay body}

: ping

event: replay
data: {replay body}
```

The first event is `snapshot` if the request offset was -1, else
`replay`. Subsequent `replay` events are pushed as new messages
arrive. Lines starting with `:` are heartbeats that double as
disconnect probes. The browser `ShapeClient` opts in via
`new ShapeClient(url, { transport: "sse" })`.

## Compact

`compact(table, retention)` deletes log rows whose `seq` is at or below
the watermark and writes the watermark to `retained_offset:<table>`.
After compaction, any client whose stored `offset` is below the
watermark must receive `409 resync_required` on its next replay.

`compact` and `shutdown` interact: a compaction in progress when
`shutdown()` is called runs to completion. `shutdown_timeout_ms`
bounds live waiters, not compaction.

## Multi-process caveat

The engine is designed for a single writer process per database.
Bootstrap clears the `current_batch_id` meta row to recover from a
crashed `write_batch`. **If two processes open the same database
concurrently, one's bootstrap may invalidate the other's in-flight
batch.** Use one writer process per database; readers in other
processes are fine.

## Conformance test cases

Every engine implements a test suite that exercises these cases:

1. Snapshot returns matching rows ordered by key columns.
2. Snapshot is filtered by `eq` predicate.
3. Snapshot is filtered by `in` predicate.
4. Snapshot is filtered by `gt`/`lt`/`gte`/`lte` predicate.
5. Snapshot is filtered by `and` predicate combining two conditions.
6. Replay emits `insert` / `update` / `delete` messages in order.
7. Replay groups statements from `write_batch` under one `batch_id`.
8. Replay extends past the requested limit to finish a batch.
9. A primary-key change replays as `delete` + `insert`.
10. `authorize` returning false yields `404 shape_not_found`.
11. Mismatched `log_id` yields `409 resync_required`.
12. Replaying past the retained offset (after `compact`) yields
    `409 resync_required`.
13. `install_triggers` on a table with no primary key fails.
14. Composite primary keys are exposed in `key_columns` and in message
    `key` objects.
15. Live wait wakes when a write commits.

The engines may add language-specific tests, but each must pass these.
