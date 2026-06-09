# Changelog

## Unreleased

Electrolite is now a Node-only TypeScript library for Electric-style
SQLite sync.

### Security & correctness (adversarial review fixes)

- **Cache safety:** snapshot/replay responses now default to
  `cache-control: private` so a shared cache/CDN can no longer serve one
  user's authorized Shape to another without `authorize()` running.
  Shapes opt into `public` caching with `cacheable: true` (all 5
  engines). [F2]
- **Compaction:** compacting a table to keep more rows than it has no
  longer deletes that table's whole log or forces its subscribers to
  resync because an unrelated table advanced the sequence; the retained
  offset also never regresses (all 5 engines). [F1]
- **Resync gate:** a replay/live request (`offset >= 0`) now requires a
  `log_id`, and the server validates a presented `shape_handle`, both
  returning `409 resync_required` on mismatch (all 5 engines). [F5, F8]
- **SQL params:** numbered SQLite placeholders (`?1`) can now be reused
  and reordered in `execute`/`writeBatch` (removed a normalizer that
  broke them). [F3]
- **Multi-tab:** leadership now uses the Web Locks API for true mutual
  exclusion (localStorage lease is a fallback), so two tabs can no
  longer both poll. [F6]
- **Diff replay:** a `replica=diff` UPDATE for a key the client doesn't
  hold now forces a clean resync instead of materializing a partial
  row. [F7]
- **Wake fanout:** a write only replay-scans live Shapes on the table
  that actually changed, instead of one scan per active Shape. [F9]

### Added

- Embedded Node package using Node's built-in SQLite API.
- Server-defined Shapes with route params, auth hooks, column allowlists,
  auth scopes, and schema versions.
- SQLite trigger installation for inserts, updates, deletes, non-`id`
  primary keys, and composite primary keys.
- Durable `_electrolite_log` and `_electrolite_meta` tables with indexed
  replay reads.
- Initial snapshots with `log_id`, `shape_handle`, key-column metadata,
  and continuation offsets.
- Replay responses for inserts, updates, deletes, primary-key changes,
  bounded pages, and `409 resync_required`.
- Live long-polling with targeted wakeups for affected Shapes.
- Explicit Electrolite write batches with shared `batch_id` and replay
  boundaries.
- Predicate support for `all`, `eq`, `in`, `and`, `null`, booleans, and
  SQLite type-policy validation.
- Browser `ShapeClient` with IndexedDB persistence, `log_id` and
  `shape_handle` validation, replay draining, retry/backoff, status
  events, low-level replay events, and multi-tab coordination.
- Tiny console demo and two-column web app demo.
- Local live fanout demo, defaulting to 100 in-process Shape clients and
  configurable with `ELECTROLITE_FANOUT_CLIENTS`.
- README demo screenshot.

### Changed

- Removed the Rust/native implementation from the active project after
  the Node implementation passed the same public behavior tests.
- Removed the older TypeScript-to-internal-origin bridge from the active
  project.
- Simplified setup to Node 24+ with no native build or sidecar.
- Converted the Node package implementation from JavaScript plus
  hand-written declarations to TypeScript source.

### Verified

- Browser client test coverage for materialization, cache validation,
  replay draining, retry/status, persistence, and multi-tab state.
- Node backend end-to-end coverage for dynamic authorized Shapes,
  snapshot/replay/live, predicates, key metadata, retention, batches,
  rollback behavior, and browser integration.
