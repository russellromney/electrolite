import os
import tempfile
import threading
import time
import unittest

from electrolite import create_electrolite, eq, shape


def setup(live_timeout_ms=100):
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "app.db")
    app = create_electrolite(
        db_path,
        live_timeout_ms=live_timeout_ms,
        shapes={
            "projectTodos": shape(
                table="todos",
                columns=["id", "project_id", "title", "done"],
                params=["project_id"],
                where=lambda ctx: eq("project_id", ctx["params"]["project_id"]),
                scope=lambda ctx: "project:" + ctx["params"]["project_id"],
                authorize=lambda ctx: ctx["params"]["project_id"] in ctx["context"]["projects"],
            )
        },
    )
    app.execute_batch(
        """
        CREATE TABLE todos (
          id INTEGER PRIMARY KEY,
          project_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done BOOLEAN NOT NULL DEFAULT 0
        );
        INSERT INTO todos (id, project_id, title, done) VALUES
          (1, 'p1', 'ship electrolite', 0),
          (2, 'p2', 'other project', 0);
        """
    )
    app.install_triggers("todos")
    return tmp, app


class PythonEngineTest(unittest.TestCase):
    def test_snapshot_and_auth(self):
        tmp, app = setup()
        self.addCleanup(tmp.cleanup)

        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            "offset=-1",
            context={"projects": {"p1"}},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["type"], "snapshot")
        self.assertEqual(body["key_columns"], ["id"])
        self.assertEqual(
            body["rows"],
            [{"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0}],
        )
        self.assertRegex(body["shape_handle"], r"^[a-f0-9]{64}$")

        status, denied = app.handle(
            "/electrolite/v1/projectTodos/p1",
            "offset=-1",
            context={"projects": {"p2"}},
        )
        self.assertEqual(status, 404)
        self.assertEqual(denied, {"error": "shape_not_found"})

    def test_replay_insert_update_delete_and_batch(self):
        tmp, app = setup()
        self.addCleanup(tmp.cleanup)

        _, snapshot = app.handle(
            "/electrolite/v1/projectTodos/p1",
            "offset=-1",
            context={"projects": {"p1"}},
        )

        app.execute(
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            [3, "p1", "from python",],
        )
        _, replay = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snapshot['offset']}&log_id={snapshot['log_id']}&shape_handle={snapshot['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual([message["type"] for message in replay["messages"]], ["insert"])
        self.assertEqual(replay["messages"][0]["value"]["title"], "from python")

        app.execute("UPDATE todos SET title = ? WHERE id = ?", ["renamed", 3])
        _, replay = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={replay['offset']}&log_id={snapshot['log_id']}&shape_handle={snapshot['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual([message["type"] for message in replay["messages"]], ["update"])
        self.assertEqual(replay["messages"][0]["value"]["title"], "renamed")

        app.execute("DELETE FROM todos WHERE id = ?", [3])
        _, replay = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={replay['offset']}&log_id={snapshot['log_id']}&shape_handle={snapshot['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual([message["type"] for message in replay["messages"]], ["delete"])

        app.write_batch(
            [
                ("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [4, "p1", "one"]),
                ("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [5, "p1", "two"]),
            ]
        )
        _, replay = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={replay['offset']}&log_id={snapshot['log_id']}&shape_handle={snapshot['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual([message["type"] for message in replay["messages"]], ["insert", "insert"])
        self.assertEqual(len({message["batch_id"] for message in replay["messages"]}), 1)

    def test_live_wait_wakes_on_change(self):
        tmp, app = setup(live_timeout_ms=500)
        self.addCleanup(tmp.cleanup)
        _, snapshot = app.handle(
            "/electrolite/v1/projectTodos/p1",
            "offset=-1",
            context={"projects": {"p1"}},
        )
        result = {}

        def wait_for_change():
            result["response"] = app.handle(
                "/electrolite/v1/projectTodos/p1",
                f"offset={snapshot['offset']}&live=true&log_id={snapshot['log_id']}&shape_handle={snapshot['shape_handle']}",
                context={"projects": {"p1"}},
            )

        thread = threading.Thread(target=wait_for_change)
        thread.start()
        time.sleep(0.05)
        app.execute(
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            [3, "p1", "live python"],
        )
        thread.join(1)

        status, body = result["response"]
        self.assertEqual(status, 200)
        self.assertEqual(body["messages"][0]["type"], "insert")
        self.assertEqual(body["messages"][0]["value"]["title"], "live python")


if __name__ == "__main__":
    unittest.main()
