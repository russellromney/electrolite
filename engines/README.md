# Electrolite engines

Electrolite is meant to be a tiny protocol with tiny embedded engines.
The browser protocol stays the same; each backend language just installs
SQLite triggers, serves snapshots, and replays live logical changes.

- [TypeScript / Node](../packages/electrolite-node/README.md) is the main engine.
- [Python](python/README.md) — small stdlib `sqlite3` engine for Flask-style apps.
- [Rust](rust/README.md) — experimental, uses `rusqlite`.
- [Go](go/README.md) — experimental, uses `modernc.org/sqlite` (pure Go, no cgo).
- [Elixir](elixir/README.md) — experimental, uses `exqlite` inside a `GenServer`.

The Rust, Go, and Elixir engines exist to prove the protocol is small
and portable. They implement triggers, snapshot, replay, and shared
write batches; they intentionally do not ship an HTTP layer or every
feature of the Node engine.
