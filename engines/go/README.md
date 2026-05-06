# Electrolite for Go

Tiny experimental Electrolite engine for Go apps using
[`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) (pure Go,
no cgo).

```go
import (
    "encoding/json"
    "net/http"

    "github.com/russellromney/electrolite/engines/go"
)

app, _ := electrolite.Open("app.db")
_ = app.ExecBatch(`
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0
  );
`)
_ = app.InstallTriggers("todos")

shape := electrolite.Shape{
    Table:     "todos",
    Columns:   []string{"id", "project_id", "title", "done"},
    Predicate: electrolite.Eq("project_id", "p1"),
}

http.HandleFunc("/electrolite/v1/projectTodos/p1", func(w http.ResponseWriter, r *http.Request) {
    snap, _ := app.Snapshot(shape)
    json.NewEncoder(w).Encode(snap)
})
```

This engine implements the conformance contract in
[`engines/PROTOCOL.md`](../PROTOCOL.md). Wire `Snapshot` / `Replay`
into whatever HTTP framework you like, or use the included
`Handle(path, query, context)` for a drop-in shape registry.

## Recommended PRAGMAs

The engine does not issue `PRAGMA` statements; the user owns those.
For production-shaped apps:

```go
db, _ := sql.Open("sqlite", "app.db")
db.Exec("PRAGMA journal_mode = WAL")
db.Exec("PRAGMA synchronous = NORMAL")
db.Exec("PRAGMA busy_timeout = 5000")
db.Close()
```

Set these once before opening the engine on the same file.

Run the tests:

```sh
cd engines/go && go test ./...
```

Experimental.
