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

This engine implements the same SQLite trigger log, snapshot, replay,
and shared-batch shape used by the Node and Python engines. There is no
HTTP layer here — wire it into whatever Rust web framework you like.

Run the tests:

```sh
cargo test --manifest-path engines/rust/Cargo.toml
```

Experimental.
