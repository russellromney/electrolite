# System

Electrolite is an experimental Electric-style reactive sync layer for
SQLite. A backend defines server-owned Shapes (table + columns +
predicate + auth scope); the browser gets an initial snapshot then
long-polls for replay messages.

## Current baseline

- Stack:
  - Five backend engines: Node (canonical), Python, Rust, Go, Elixir.
  - One client today: the browser `ShapeClient` (vanilla JS, IndexedDB
    persistence, multi-tab leadership).
  - SQLite is the only datastore. Each engine installs triggers that
    write a logical change log into `_electrolite_log`.
- The system currently does:
  - **Engines**: every engine implements the conformance contract in
    `engines/PROTOCOL.md` — install triggers, snapshot, replay,
    `write_batch` with shared `batch_id`, live wait, range/eq/in/and
    predicates, log-id and retained-offset resync, `compact()`.
  - **HTTP transport**: long-polling. Live waits hold one connection
    per subscriber for `live_timeout_ms`.
  - **Browser client**: snapshot/replay materialization, IndexedDB
    cache, multi-tab leadership, `log_id`/`shape_handle` validation.
  - **Test matrix**: `tests/matrix.test.ts` spawns every engine over
    real HTTP and drives the browser `ShapeClient` against each.
    Today: 5 engines × 1 client = 5 cells, all green.
- Wire parity is **proven** as of phase 0001: every engine produces
  byte-identical `shape_handle` for the same shape; every engine
  returns 400 / 404 / 409 the same way. Verified by the conformance
  harness at `engines/conformance/`.
- Engines support a `shutdown()` API that wakes live waiters and
  closes the connection.
- Two transports are supported: long-poll (default) and SSE. The
  browser `ShapeClient` selects via `transport: "sse"`. Every
  engine's test server speaks both. The matrix runs the full
  scenario (snapshot, insert/update/delete, batch, predicate
  boundary) over each transport against each engine: 5 × 2 = 10
  cells, all green.
- The cross-engine in-process predicate matcher is exercised by
  the conformance harness; SQL `WHERE` and the matcher agree on
  every engine.
- The system does not yet do:
  - HTTP/2 push and WebSocket adapter (out of scope; SSE is the
    streaming transport).
  - **Connection pool / WAL-mode-aware concurrency.** Every engine
    is single-connection serialized. PRAGMAs are the user's
    responsibility (engines do not force `journal_mode`).
  - **Cross-process wake.** `update_hook` (Rust) only sees this
    connection's writes; other engines rely on engine-internal notify.
  - **Backend client libraries.** Browser is the only client.

## Boundaries that matter

- **Server-defined shapes only.** Browser cannot send SQL. Predicates
  are a structured JSON union: `all`, `eq`, `in`, `gt`/`lt`/`gte`/
  `lte`, `and`. The shape's `where`/`scope`/`authorize` callbacks run
  in app code with the request context.
- **`shape_handle` is identity.** Defined as
  `sha256(canonical_json(normalized_shape))` where the normalized
  shape is `{auth_scope, columns (sorted), predicate (normalized),
  schema_version, table}`. Every engine must produce identical bytes
  for the same shape. Today, Node does not — see Phase 0001.
- **`log_id` and retained offset gate resync.** A client whose
  presented `log_id` or `offset` is stale gets `409 resync_required`
  and re-snapshots.
- **Auth at the shape boundary.** `authorize()` returning false yields
  `404 shape_not_found`, deliberately indistinguishable from an
  unknown shape name.
- **One writer per engine.** SQLite supports one writer at a time;
  engines do not virtualize this. Multiple readers are not supported
  yet (single connection, mutex-serialized).

## Non-goals

- Postgres replication. Cross-database sync.
- Arbitrary client-provided SQL.
- Offline writes / conflict resolution in the first version.
- A required standalone sync daemon. Engines run embedded.
- Multi-host clustering. SQLite-file-per-process is the model.

## Notes

- Only the proved baseline goes here. Phases live in `.intent/phases/`
  and `ROADMAP.md`.
- Update this file only after a phase's proof lands.
