"""Electrolite engine conformance tests.

Covers the cases listed in engines/PROTOCOL.md. Every engine should
exercise the same behaviors; this file is the Python copy.
"""

import os
import tempfile
import threading
import time
import unittest

from electrolite import and_, create_electrolite, eq, gt, in_list, shape


def _setup(live_timeout_ms=100, shapes=None):
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "app.db")
    if shapes is None:
        shapes = {
            "projectTodos": shape(
                table="todos",
                columns=["id", "project_id", "title", "done"],
                params=["project_id"],
                where=lambda ctx: eq("project_id", ctx["params"]["project_id"]),
                scope=lambda ctx: "project:" + ctx["params"]["project_id"],
                authorize=lambda ctx: ctx["params"]["project_id"] in ctx["context"]["projects"],
            )
        }
    app = create_electrolite(db_path, live_timeout_ms=live_timeout_ms, shapes=shapes)
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
          (2, 'p2', 'other project', 0),
          (3, 'p1', 'second p1', 0);
        """
    )
    app.install_triggers("todos")
    return tmp, app


class SnapshotConformance(unittest.TestCase):
    def test_snapshot_returns_matching_rows_ordered_by_key(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            "offset=-1",
            context={"projects": {"p1"}},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["type"], "snapshot")
        self.assertEqual([row["id"] for row in body["rows"]], [1, 3])
        self.assertEqual(body["key_columns"], ["id"])
        self.assertRegex(body["shape_handle"], r"^[a-f0-9]{64}$")
        self.assertRegex(body["log_id"], r"^[a-f0-9]{32}$")

    def test_eq_predicate_filters_snapshot(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p2",
            "offset=-1",
            context={"projects": {"p2"}},
        )
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in body["rows"]], [2])

    def test_in_predicate_filters_snapshot(self):
        shapes = {
            "todoSet": shape(
                table="todos",
                columns=["id", "project_id"],
                where=lambda ctx: in_list("project_id", ["p1", "p2"]),
            )
        }
        tmp, app = _setup(shapes=shapes)
        self.addCleanup(tmp.cleanup)
        status, body = app.handle("/electrolite/v1/todoSet", "offset=-1")
        self.assertEqual(status, 200)
        self.assertEqual({row["id"] for row in body["rows"]}, {1, 2, 3})

    def test_range_predicate_filters_snapshot(self):
        shapes = {
            "highIds": shape(
                table="todos",
                columns=["id", "project_id"],
                where=lambda ctx: gt("id", 1),
            )
        }
        tmp, app = _setup(shapes=shapes)
        self.addCleanup(tmp.cleanup)
        status, body = app.handle("/electrolite/v1/highIds", "offset=-1")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in body["rows"]], [2, 3])

    def test_and_predicate_combines_conditions(self):
        shapes = {
            "p1HighIds": shape(
                table="todos",
                columns=["id", "project_id"],
                where=lambda ctx: and_(eq("project_id", "p1"), gt("id", 1)),
            )
        }
        tmp, app = _setup(shapes=shapes)
        self.addCleanup(tmp.cleanup)
        status, body = app.handle("/electrolite/v1/p1HighIds", "offset=-1")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in body["rows"]], [3])


class ReplayConformance(unittest.TestCase):
    def test_replay_emits_insert_update_delete(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )

        app.execute("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [10, "p1", "new"])
        app.execute("UPDATE todos SET title = ? WHERE id = ?", ["renamed", 10])
        app.execute("DELETE FROM todos WHERE id = ?", [10])

        _, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snap['offset']}&log_id={snap['log_id']}&shape_handle={snap['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual([m["type"] for m in body["messages"]], ["insert", "update", "delete"])

    def test_write_batch_shares_batch_id(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        app.write_batch(
            [
                ("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [11, "p1", "a"]),
                ("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [12, "p1", "b"]),
            ]
        )
        _, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snap['offset']}&log_id={snap['log_id']}&shape_handle={snap['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual([m["type"] for m in body["messages"]], ["insert", "insert"])
        self.assertEqual(len({m["batch_id"] for m in body["messages"]}), 1)

    def test_replay_extends_past_limit_to_finish_batch(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        # replay_limit is configured at construction; bypass via direct replay
        from electrolite import shape_handle as _sh
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        app.write_batch(
            [
                ("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [21, "p1", "a"]),
                ("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [22, "p1", "b"]),
                ("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [23, "p1", "c"]),
            ]
        )
        # call replay() directly with limit=1; engine must extend to include the rest of the batch
        spec = {
            "table": "todos",
            "columns": ["id", "project_id", "title", "done"],
            "predicate": eq("project_id", "p1"),
            "auth_scope": "",
            "schema_version": 1,
        }
        out = app.replay(spec, snap["offset"], 1)
        self.assertEqual(len(out["messages"]), 3)
        self.assertEqual(len({m["batch_id"] for m in out["messages"]}), 1)

    def test_pk_change_replays_as_delete_then_insert(self):
        # Use a rowid-like table to make pk update cleanly observable.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "app.db")
        app = create_electrolite(
            db_path,
            shapes={
                "thingsByOwner": shape(
                    table="things",
                    columns=["thing_id", "owner"],
                    params=["owner"],
                    where=lambda ctx: eq("owner", ctx["params"]["owner"]),
                )
            },
        )
        app.execute_batch(
            """
            CREATE TABLE things (
              thing_id TEXT NOT NULL PRIMARY KEY,
              owner TEXT NOT NULL
            );
            INSERT INTO things (thing_id, owner) VALUES ('a', 'alice');
            """
        )
        app.install_triggers("things")
        _, snap = app.handle("/electrolite/v1/thingsByOwner/alice", "offset=-1")
        app.execute("UPDATE things SET thing_id = ? WHERE thing_id = ?", ["b", "a"])
        _, body = app.handle(
            "/electrolite/v1/thingsByOwner/alice",
            f"offset={snap['offset']}&log_id={snap['log_id']}&shape_handle={snap['shape_handle']}",
        )
        self.assertEqual([m["type"] for m in body["messages"]], ["delete", "insert"])
        self.assertEqual(body["messages"][0]["key"], {"thing_id": "a"})
        self.assertEqual(body["messages"][1]["key"], {"thing_id": "b"})


class AuthAndResyncConformance(unittest.TestCase):
    def test_unauthorized_shape_returns_404_shape_not_found(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            "offset=-1",
            context={"projects": {"p2"}},
        )
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "shape_not_found"})

    def test_log_id_mismatch_returns_409(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snap['offset']}&log_id=00000000000000000000000000000000",
            context={"projects": {"p1"}},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "resync_required"})

    def test_missing_log_id_on_replay_returns_409(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        # Replay request (offset >= 0) without a log_id query param must 409.
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snap['offset']}&shape_handle={snap['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "resync_required"})

    def test_shape_handle_mismatch_returns_409(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snap['offset']}&log_id={snap['log_id']}&shape_handle=deadbeef",
            context={"projects": {"p1"}},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "resync_required"})

    def test_absent_shape_handle_is_lenient(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        # log_id present, shape_handle absent → no 409.
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snap['offset']}&log_id={snap['log_id']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["type"], "replay")

    def test_unauthorized_takes_precedence_over_missing_log_id(self):
        # Authorization (404) is checked before the offset>=0 resync
        # checks, so an unauthorized replay request still yields 404.
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            "offset=5",
            context={"projects": {"p2"}},
        )
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "shape_not_found"})

    def test_compact_makes_old_offsets_resync(self):
        tmp, app = _setup()
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        # generate some history then compact aggressively
        app.execute("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [50, "p1", "x"])
        result = app.compact("todos", keep_last=0)
        self.assertGreaterEqual(result["retained_offset"], snap["offset"])
        status, body = app.handle(
            "/electrolite/v1/projectTodos/p1",
            f"offset={snap['offset']}&log_id={snap['log_id']}&shape_handle={snap['shape_handle']}",
            context={"projects": {"p1"}},
        )
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "resync_required"})


class CompactionWatermarkConformance(unittest.TestCase):
    def _two_table_app(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "app.db")
        app = create_electrolite(
            db_path,
            shapes={
                "todos": shape(table="todos", columns=["id", "title"]),
                "events": shape(table="events", columns=["id", "kind"]),
            },
        )
        app.execute_batch(
            """
            CREATE TABLE todos (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
            CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL);
            """
        )
        app.install_triggers("todos")
        app.install_triggers("events")
        return app

    def test_compact_does_not_use_global_high_water_mark(self):
        app = self._two_table_app()
        # A few todos rows, then MANY events rows so the global max(seq)
        # is far ahead of todos' own max seq.
        for i in range(1, 4):
            app.execute("INSERT INTO todos (id, title) VALUES (?, ?)", [i, f"t{i}"])
        todos_max = app.db.execute(
            "SELECT MAX(seq) AS s FROM _electrolite_log WHERE table_name = 'todos'"
        ).fetchone()["s"]
        for i in range(1, 200):
            app.execute("INSERT INTO events (id, kind) VALUES (?, ?)", [i, "e"])
        self.assertGreater(app.high_water_mark(), todos_max)

        # keep_last larger than todos' row count → keep everything.
        result = app.compact("todos", 1000)
        self.assertEqual(result["deleted_rows"], 0)
        self.assertEqual(result["retained_offset"], 0)
        remaining = app.db.execute(
            "SELECT COUNT(*) AS c FROM _electrolite_log WHERE table_name = 'todos'"
        ).fetchone()["c"]
        self.assertEqual(remaining, 3)

    def test_compact_retained_offset_is_monotonic(self):
        app = self._two_table_app()
        for i in range(1, 6):
            app.execute("INSERT INTO todos (id, title) VALUES (?, ?)", [i, f"t{i}"])
        todos_max = app.db.execute(
            "SELECT MAX(seq) AS s FROM _electrolite_log WHERE table_name = 'todos'"
        ).fetchone()["s"]

        # keep_last=0 retains up to todos' max seq.
        first = app.compact("todos", 0)
        self.assertEqual(first["retained_offset"], todos_max)

        # A later compact with a huge keep_last must NOT lower the
        # retained offset back down to 0.
        second = app.compact("todos", 1000)
        self.assertEqual(second["retained_offset"], todos_max)
        self.assertEqual(second["deleted_rows"], 0)


class TableConformance(unittest.TestCase):
    def test_install_triggers_requires_primary_key(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "app.db")
        app = create_electrolite(db_path, shapes={})
        app.execute_batch("CREATE TABLE no_pk (a INTEGER, b INTEGER);")
        with self.assertRaises(ValueError):
            app.install_triggers("no_pk")

    def test_composite_primary_keys_exposed_and_used_in_keys(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "app.db")
        app = create_electrolite(
            db_path,
            shapes={
                "memberships": shape(
                    table="memberships",
                    columns=["org", "user", "role"],
                )
            },
        )
        app.execute_batch(
            """
            CREATE TABLE memberships (
              org TEXT NOT NULL,
              user TEXT NOT NULL,
              role TEXT NOT NULL,
              PRIMARY KEY (org, user)
            );
            INSERT INTO memberships (org, user, role) VALUES ('o1', 'u1', 'admin');
            """
        )
        app.install_triggers("memberships")
        _, snap = app.handle("/electrolite/v1/memberships", "offset=-1")
        self.assertEqual(snap["key_columns"], ["org", "user"])
        self.assertEqual(snap["rows"], [{"org": "o1", "user": "u1", "role": "admin"}])
        app.execute("DELETE FROM memberships WHERE org = ? AND user = ?", ["o1", "u1"])
        _, body = app.handle(
            "/electrolite/v1/memberships",
            f"offset={snap['offset']}&log_id={snap['log_id']}&shape_handle={snap['shape_handle']}",
        )
        self.assertEqual(body["messages"][0]["type"], "delete")
        self.assertEqual(body["messages"][0]["key"], {"org": "o1", "user": "u1"})


class RobustnessConformance(unittest.TestCase):
    def test_stale_current_batch_id_cleared_on_bootstrap(self):
        # Simulate a crashed write_batch by writing current_batch_id
        # directly, then re-bootstrapping. The next ordinary write
        # must NOT inherit that dead batch_id.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "app.db")
        app = create_electrolite(db_path)
        app.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);"
        )
        app.install_triggers("t")

        # Plant a stale batch_id directly in meta and verify a fresh
        # connection (re-bootstrap) clears it.
        app.db.execute(
            """
            INSERT INTO _electrolite_meta (key, value) VALUES ('current_batch_id', 'STALE')
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """
        )
        app.db.commit()
        app.shutdown()

        app2 = create_electrolite(db_path)
        # bootstrap clears current_batch_id; the next insert should
        # pick a fresh random batch via lower(hex(randomblob(16))).
        app2.execute("INSERT INTO t (id, v) VALUES (?, ?)", [1, "hello"])
        row = app2.db.execute(
            "SELECT batch_id FROM _electrolite_log WHERE table_name = 't' ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        self.assertNotEqual(row["batch_id"], "STALE")
        self.assertRegex(row["batch_id"], r"^[a-f0-9]{32}$")

    def test_replay_batch_cap_signals_more_to_come(self):
        # Insert a batch larger than 10 * replay_limit and verify the
        # response page is bounded and signals up_to_date=false.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "app.db")
        app = create_electrolite(db_path)
        app.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);"
        )
        app.install_triggers("t")

        statements = [
            ("INSERT INTO t (id, v) VALUES (?, ?)", [i, f"row-{i}"])
            for i in range(1, 25)
        ]
        app.write_batch(statements)

        spec = {
            "table": "t",
            "columns": ["id", "v"],
            "predicate": {"type": "all"},
            "auth_scope": "",
            "schema_version": 1,
        }
        # replay_limit=1 → extension cap = 10 → page should yield 11
        # rows (initial 1 + 10 extension), and up_to_date=False since
        # the batch isn't fully drained.
        body = app.replay(spec, 0, 1)
        self.assertEqual(len(body["messages"]), 11)
        self.assertFalse(body["up_to_date"])
        # Continue: next page should also be capped.
        body2 = app.replay(spec, body["offset"], 1)
        self.assertEqual(len(body2["messages"]), 11)
        # Final page drains the rest.
        body3 = app.replay(spec, body2["offset"], 1)
        self.assertGreater(len(body3["messages"]), 0)
        self.assertTrue(body3["up_to_date"])

    def test_shutdown_wakes_live_waiters(self):
        # A live waiter should be woken by shutdown() so the request
        # returns instead of hanging until the timeout.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = os.path.join(tmp.name, "app.db")
        app = create_electrolite(
            db_path,
            live_timeout_ms=10_000,
            shapes={
                "all": shape(
                    table="t",
                    columns=["id", "v"],
                )
            },
        )
        app.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);"
        )
        app.install_triggers("t")

        # Pin to current offset so the live wait triggers.
        _, snap = app.handle("/electrolite/v1/all", "offset=-1")
        result = {}
        started = threading.Event()

        def wait_for_change():
            started.set()
            result["response"] = app.handle(
                "/electrolite/v1/all",
                f"offset={snap['offset']}&live=true&log_id={snap['log_id']}&shape_handle={snap['shape_handle']}",
            )

        thread = threading.Thread(target=wait_for_change)
        thread.start()
        started.wait()
        time.sleep(0.05)

        start = time.monotonic()
        app.shutdown()
        thread.join(2)
        elapsed = time.monotonic() - start

        self.assertFalse(thread.is_alive())
        self.assertLess(elapsed, 1.0, "shutdown should wake waiter immediately, not at timeout")
        status, _ = result["response"]
        self.assertIn(status, (200, 409, 500))


class LiveWaitConformance(unittest.TestCase):
    def test_live_wait_wakes_when_write_commits(self):
        tmp, app = _setup(live_timeout_ms=500)
        self.addCleanup(tmp.cleanup)
        _, snap = app.handle(
            "/electrolite/v1/projectTodos/p1", "offset=-1", context={"projects": {"p1"}}
        )
        result = {}

        def wait():
            result["response"] = app.handle(
                "/electrolite/v1/projectTodos/p1",
                f"offset={snap['offset']}&live=true&log_id={snap['log_id']}&shape_handle={snap['shape_handle']}",
                context={"projects": {"p1"}},
            )

        thread = threading.Thread(target=wait)
        thread.start()
        time.sleep(0.05)
        app.execute("INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [60, "p1", "live"])
        thread.join(1)

        status, body = result["response"]
        self.assertEqual(status, 200)
        self.assertEqual(body["messages"][0]["type"], "insert")
        self.assertEqual(body["messages"][0]["value"]["title"], "live")


if __name__ == "__main__":
    unittest.main()
