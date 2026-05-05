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

This engine implements the same SQLite trigger log, snapshot, replay,
and shared-batch shape as the Node and Python engines. Wire `snapshot`
and `replay` into Plug or Phoenix as you like.

Run the tests:

```sh
cd engines/elixir && mix deps.get && mix test
```

Experimental.
