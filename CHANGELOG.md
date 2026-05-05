# Changelog

## Unreleased

Electrolite is now a Node-only TypeScript library for Electric-style
SQLite sync.

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
- Python materializer client for consuming Shape HTTP endpoints.
- Tiny console demo and two-column web app demo.

### Changed

- Removed the Rust/native implementation from the active project after
  the Node implementation passed the same public behavior tests.
- Removed the older TypeScript-to-internal-origin bridge from the active
  project.
- Simplified setup to Node 24+ with no native build or sidecar.

### Verified

- Browser client test coverage for materialization, cache validation,
  replay draining, retry/status, persistence, and multi-tab state.
- Node backend end-to-end coverage for dynamic authorized Shapes,
  snapshot/replay/live, predicates, key metadata, retention, batches,
  rollback behavior, and browser integration.
