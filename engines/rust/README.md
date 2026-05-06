# Electrolite for Rust

Tiny experimental Electrolite engine for Rust apps using
[`rusqlite`](https://crates.io/crates/rusqlite). A proof that the
Electrolite protocol — install triggers, snapshot, replay, batch — is
small enough to fit in a few hundred lines of any language.

```rust
use electrolite::{Electrolite, Predicate, Shape};
use serde_json::json;

let app = Electrolite::open("app.db")?;
app.execute_batch(r#"
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0
  );
"#)?;
app.install_triggers("todos")?;

let shape = Shape {
    table: "todos".into(),
    columns: vec!["id".into(), "project_id".into(), "title".into(), "done".into()],
    predicate: Predicate::Eq { column: "project_id".into(), value: json!("p1") },
};

let snap = app.snapshot(&shape)?;
let replay = app.replay(&shape, snap.offset, 1000)?;
```

This engine implements the conformance contract in
[`engines/PROTOCOL.md`](../PROTOCOL.md): triggers, snapshot, replay,
shared-batch writes, log-id and retained-offset resync, range / eq /
in / and / boolean-coerced predicates. The included `handle(path,
query, context)` method serves shapes over HTTP, but you can wire
the lower-level `snapshot` / `replay` API into any Rust framework.

## Live wait and `update_hook`

The engine subscribes to SQLite's `update_hook` on its own connection
to drive live waits. **The hook only sees writes on this connection.**
If a different process or a separate `Connection` writes to the same
SQLite file, live waiters will not be woken — they will time out
after `live_timeout` and return their normal poll-style replay.
Single-process embedded use is the supported model.

## Recommended PRAGMAs

The engine does not issue `PRAGMA` statements; the user owns those.
A useful default for production-shaped apps:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
```

Set these before calling `Electrolite::open` (e.g., on a connection
held open during `pragma_update`).

Run the tests:

```sh
cargo test --manifest-path engines/rust/Cargo.toml
```

Experimental.
