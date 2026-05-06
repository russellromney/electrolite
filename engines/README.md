# Electrolite engines

Electrolite is a tiny protocol with embedded engines. The browser
protocol stays the same; each engine just installs SQLite triggers,
serves snapshots over HTTP, and replays live logical changes.

## Engines

- [TypeScript / Node](../packages/electrolite-node/README.md) — main engine, also the reference for browser-client integration tests.
- [Python](python/README.md) — stdlib `sqlite3`, suits Flask-style apps.
- [Rust](rust/README.md) — `rusqlite` with SQLite `update_hook` for in-process push wakes.
- [Go](go/README.md) — `modernc.org/sqlite` (pure Go, no cgo).
- [Elixir](elixir/README.md) — `exqlite` inside a `GenServer`.

## Parity

Every engine implements the same protocol. The behavior contract lives
in [PROTOCOL.md](PROTOCOL.md) and every engine ships a conformance
suite that exercises each case.

| Behavior | Node | Python | Rust | Go | Elixir |
|---|---|---|---|---|---|
| Snapshot ordered by key | ✓ | ✓ | ✓ | ✓ | ✓ |
| `eq` / `in` / `and` / range predicates | ✓ | ✓ | ✓ | ✓ | ✓ |
| `insert` / `update` / `delete` replay | ✓ | ✓ | ✓ | ✓ | ✓ |
| Shared `batch_id` across `write_batch` | ✓ | ✓ | ✓ | ✓ | ✓ |
| Replay extends past limit to finish a batch | ✓ | ✓ | ✓ | ✓ | ✓ |
| PK update replays as `delete` + `insert` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `authorize` returning false → `404 shape_not_found` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `log_id` mismatch → `409 resync_required` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `compact()` past offset → `409 resync_required` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `install_triggers` requires primary key | ✓ | ✓ | ✓ | ✓ | ✓ |
| Composite primary keys exposed in `key` objects | ✓ | ✓ | ✓ | ✓ | ✓ |
| Live wait wakes when a write commits | ✓ | ✓ | ✓ | ✓ | ✓ |
| Browser `ShapeClient` end-to-end | ✓ | — | — | — | — |

The browser-client end-to-end test is intrinsically a Node-side
integration — the `ShapeClient` lives in `clients/browser/electrolite.js`
and runs against the Node engine in the same process. Other engines
serve the same protocol over HTTP, so the same browser client works
against them; the integration is just easiest to drive from Node.

## Running the suites

```sh
npm run test:node
npm run test:python
npm run test:rust
npm run test:go
npm run test:elixir
```

`npm test` runs Node + Python + browser. `npm run test:all` adds
Rust, Go, and Elixir.

## Adding a new engine

A new engine is correct when it passes the conformance suite. Port the
test cases (see any of the existing test files) and you have parity by
construction. The protocol fits in a few hundred lines of any
reasonable language.
