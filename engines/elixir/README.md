# Electrolite for Elixir

Tiny experimental Electrolite engine for Elixir apps using
[`exqlite`](https://hex.pm/packages/exqlite). One `GenServer` owns one
SQLite connection, so concurrent calls serialize cleanly.

```elixir
{:ok, pid} = Electrolite.start_link(db_path: "app.db")

:ok = Electrolite.execute_batch(pid, """
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0
  );
""")

:ok = Electrolite.install_triggers(pid, "todos")

shape =
  Electrolite.shape(
    table: "todos",
    columns: ["id", "project_id", "title", "done"],
    predicate: Electrolite.eq("project_id", "p1")
  )

{:ok, snap} = Electrolite.snapshot(pid, shape)
{:ok, replay} = Electrolite.replay(pid, shape, snap.offset, 1000)
```

This engine implements the conformance contract in
[`engines/PROTOCOL.md`](../PROTOCOL.md). Wire `Electrolite.handle/4`
into Plug or Phoenix as you like.

## Live wait

`Exqlite` does not expose SQLite's `update_hook` to Elixir code, so
this engine relies on its own notify-on-write inside the GenServer.
**Writes that bypass `Electrolite.execute/3`** (for example, a raw
`Exqlite.Sqlite3.execute` against the same connection from another
process, or a separate process with its own connection to the same
file) **will not wake live waiters.** All writes should go through
the GenServer.

## Recommended PRAGMAs

The engine does not issue `PRAGMA` statements; the user owns those.
For production-shaped apps:

```elixir
:ok = Exqlite.Sqlite3.execute(conn, "PRAGMA journal_mode = WAL")
:ok = Exqlite.Sqlite3.execute(conn, "PRAGMA synchronous = NORMAL")
:ok = Exqlite.Sqlite3.execute(conn, "PRAGMA busy_timeout = 5000")
```

Run the tests:

```sh
cd engines/elixir && mix deps.get && mix test
```

Experimental.
