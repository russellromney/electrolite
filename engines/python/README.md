# Electrolite for Python

Tiny experimental Electrolite engine for Python apps using stdlib
`sqlite3`. Think: Electric-style Shapes for a Flask + SQLite app.

```py
from flask import Flask, jsonify, request
from electrolite import create_electrolite, eq, shape

app = Flask(__name__)

electrolite = create_electrolite(
    "app.db",
    shapes={
        "projectTodos": shape(
            table="todos",
            columns=["id", "project_id", "title", "done"],
            params=["project_id"],
            where=lambda ctx: eq("project_id", ctx["params"]["project_id"]),
            authorize=lambda ctx: ctx["params"]["project_id"] in ctx["context"]["projects"],
        )
    },
)

electrolite.execute_batch("""
CREATE TABLE IF NOT EXISTS todos (
  id INTEGER PRIMARY KEY,
  project_id TEXT NOT NULL,
  title TEXT NOT NULL,
  done BOOLEAN NOT NULL DEFAULT 0
);
""")
electrolite.install_triggers("todos")


@app.get("/electrolite/v1/<shape_name>/<project_id>")
def electrolite_shape(shape_name, project_id):
    status, body = electrolite.handle(
        f"/electrolite/v1/{shape_name}/{project_id}",
        request.query_string.decode(),
        context={"projects": {"p1"}},
    )
    return jsonify(body), status
```

## Recommended PRAGMAs

The engine does not issue `PRAGMA` statements; the user owns those.
For production-shaped apps:

```python
db = sqlite3.connect("app.db")
db.execute("PRAGMA journal_mode = WAL")
db.execute("PRAGMA synchronous = NORMAL")
db.execute("PRAGMA busy_timeout = 5000")
db.commit()
db.close()
```

Set these once when the file is first created.

Run the engine tests:

```sh
python3 -m unittest discover -s engines/python
```
