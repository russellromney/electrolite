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

This engine implements the same SQLite trigger log, snapshot, replay,
and shared-batch shape as the Node and Python engines. Wire `Snapshot`
and `Replay` into whatever HTTP framework you like.

Run the tests:

```sh
cd engines/go && go test ./...
```

Experimental.
