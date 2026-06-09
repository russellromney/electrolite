//! Electrolite Rust engine conformance tests.
//! Mirrors the cases listed in engines/PROTOCOL.md.

use electrolite::{
    all, and, eq, gt, in_list, lt, Electrolite, Predicate, RangeOp, Shape, ShapeDef,
};
use rusqlite::types::Value;
use serde_json::{json, Value as Json};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn open_app() -> (tempfile::TempDir, Electrolite) {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute_batch(
        r#"
        CREATE TABLE todos (
          id INTEGER PRIMARY KEY,
          project_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO todos (id, project_id, title, done) VALUES
          (1, 'p1', 'ship electrolite', 0),
          (2, 'p2', 'other project', 0),
          (3, 'p1', 'second p1', 0);
        "#,
    )
    .unwrap();
    app.install_triggers("todos").unwrap();
    (tmp, app)
}

fn project_todos_app() -> (tempfile::TempDir, Electrolite) {
    let (tmp, mut app) = open_app();
    app.add_shape(
        "projectTodos",
        ShapeDef::new("todos", vec!["id".into(), "project_id".into(), "title".into(), "done".into()])
            .params(vec!["project_id".into()])
            .where_fn(|ctx| eq("project_id", json!(ctx.params["project_id"].clone())))
            .scope_fn(|ctx| format!("project:{}", ctx.params["project_id"]))
            .authorize_fn(|ctx| {
                let projects = ctx.context.get("projects").and_then(|p| p.as_array());
                match projects {
                    Some(arr) => arr.iter().any(|p| p.as_str() == Some(&ctx.params["project_id"])),
                    None => false,
                }
            }),
    );
    (tmp, app)
}

#[test]
fn snapshot_returns_matching_rows_ordered_by_key() {
    let (_tmp, app) = project_todos_app();
    let (status, body) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=-1",
        &json!({"projects": ["p1"]}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["type"], "snapshot");
    let ids: Vec<i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![1, 3]);
    assert_eq!(body["key_columns"], json!(["id"]));
    assert_eq!(body["shape_handle"].as_str().unwrap().len(), 64);
}

#[test]
fn eq_predicate_filters_snapshot() {
    let (_tmp, app) = project_todos_app();
    let (status, body) = app.handle(
        "/electrolite/v1/projectTodos/p2",
        "offset=-1",
        &json!({"projects": ["p2"]}),
    );
    assert_eq!(status, 200);
    let ids: Vec<i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![2]);
}

#[test]
fn in_predicate_filters_snapshot() {
    let (_tmp, mut app) = open_app();
    app.add_shape(
        "todoSet",
        ShapeDef::new("todos", vec!["id".into(), "project_id".into()])
            .where_fn(|_| in_list("project_id", vec![json!("p1"), json!("p2")])),
    );
    let (status, body) = app.handle("/electrolite/v1/todoSet", "offset=-1", &json!({}));
    assert_eq!(status, 200);
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);
}

#[test]
fn range_predicate_filters_snapshot() {
    let (_tmp, mut app) = open_app();
    app.add_shape(
        "highIds",
        ShapeDef::new("todos", vec!["id".into(), "project_id".into()])
            .where_fn(|_| gt("id", json!(1))),
    );
    let (status, body) = app.handle("/electrolite/v1/highIds", "offset=-1", &json!({}));
    assert_eq!(status, 200);
    let ids: Vec<i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn and_predicate_combines_conditions() {
    let (_tmp, mut app) = open_app();
    app.add_shape(
        "p1HighIds",
        ShapeDef::new("todos", vec!["id".into(), "project_id".into()])
            .where_fn(|_| and(vec![eq("project_id", json!("p1")), gt("id", json!(1))])),
    );
    let (status, body) = app.handle("/electrolite/v1/p1HighIds", "offset=-1", &json!({}));
    assert_eq!(status, 200);
    let ids: Vec<i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![3]);
}

#[test]
fn replay_emits_insert_update_delete() {
    let (_tmp, app) = project_todos_app();
    let (_, snap) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=-1",
        &json!({"projects": ["p1"]}),
    );
    app.execute(
        "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
        &[Value::Integer(10), Value::Text("p1".into()), Value::Text("new".into())],
    )
    .unwrap();
    app.execute(
        "UPDATE todos SET title = ? WHERE id = ?",
        &[Value::Text("renamed".into()), Value::Integer(10)],
    )
    .unwrap();
    app.execute("DELETE FROM todos WHERE id = ?", &[Value::Integer(10)])
        .unwrap();

    let q = format!(
        "offset={}&log_id={}&shape_handle={}",
        snap["offset"].as_i64().unwrap(),
        snap["log_id"].as_str().unwrap(),
        snap["shape_handle"].as_str().unwrap(),
    );
    let (status, body) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        &q,
        &json!({"projects": ["p1"]}),
    );
    assert_eq!(status, 200, "got body: {}", body);
    let kinds: Vec<&str> = body["messages"]
        .as_array()
        .expect(&format!("messages missing: body = {}", body))
        .iter()
        .map(|m| m["type"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, vec!["insert", "update", "delete"]);
}

#[test]
fn write_batch_shares_batch_id() {
    let (_tmp, app) = project_todos_app();
    let (_, snap) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=-1",
        &json!({"projects": ["p1"]}),
    );
    app.write_batch(&[
        (
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            vec![Value::Integer(11), Value::Text("p1".into()), Value::Text("a".into())],
        ),
        (
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            vec![Value::Integer(12), Value::Text("p1".into()), Value::Text("b".into())],
        ),
    ])
    .unwrap();
    let q = format!(
        "offset={}&log_id={}&shape_handle={}",
        snap["offset"].as_i64().unwrap(),
        snap["log_id"].as_str().unwrap(),
        snap["shape_handle"].as_str().unwrap(),
    );
    let (_, body) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        &q,
        &json!({"projects": ["p1"]}),
    );
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["batch_id"], messages[1]["batch_id"]);
}

#[test]
fn replay_extends_past_limit_to_finish_batch() {
    let (_tmp, app) = project_todos_app();
    let snap = app
        .snapshot(&Shape::new(
            "todos",
            vec!["id".into(), "project_id".into()],
            eq("project_id", json!("p1")),
        ))
        .unwrap();
    app.write_batch(&[
        (
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            vec![Value::Integer(21), Value::Text("p1".into()), Value::Text("a".into())],
        ),
        (
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            vec![Value::Integer(22), Value::Text("p1".into()), Value::Text("b".into())],
        ),
        (
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            vec![Value::Integer(23), Value::Text("p1".into()), Value::Text("c".into())],
        ),
    ])
    .unwrap();
    let r = app
        .replay(
            &Shape::new(
                "todos",
                vec!["id".into(), "project_id".into()],
                eq("project_id", json!("p1")),
            ),
            snap.offset,
            1,
        )
        .unwrap();
    assert_eq!(r.messages.len(), 3);
    assert_eq!(r.messages[0]["batch_id"], r.messages[1]["batch_id"]);
    assert_eq!(r.messages[1]["batch_id"], r.messages[2]["batch_id"]);
}

#[test]
fn pk_change_replays_as_delete_then_insert() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let mut app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute_batch(
        r#"
        CREATE TABLE things (thing_id TEXT PRIMARY KEY, owner TEXT NOT NULL);
        INSERT INTO things (thing_id, owner) VALUES ('a', 'alice');
        "#,
    )
    .unwrap();
    app.install_triggers("things").unwrap();
    app.add_shape(
        "thingsByOwner",
        ShapeDef::new("things", vec!["thing_id".into(), "owner".into()])
            .params(vec!["owner".into()])
            .where_fn(|ctx| eq("owner", json!(ctx.params["owner"].clone()))),
    );

    let (_, snap) = app.handle("/electrolite/v1/thingsByOwner/alice", "offset=-1", &json!({}));
    app.execute(
        "UPDATE things SET thing_id = ? WHERE thing_id = ?",
        &[Value::Text("b".into()), Value::Text("a".into())],
    )
    .unwrap();

    let q = format!(
        "offset={}&log_id={}&shape_handle={}",
        snap["offset"].as_i64().unwrap(),
        snap["log_id"].as_str().unwrap(),
        snap["shape_handle"].as_str().unwrap(),
    );
    let (_, body) = app.handle("/electrolite/v1/thingsByOwner/alice", &q, &json!({}));
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages[0]["type"], "delete");
    assert_eq!(messages[0]["key"], json!({"thing_id": "a"}));
    assert_eq!(messages[1]["type"], "insert");
    assert_eq!(messages[1]["key"], json!({"thing_id": "b"}));
}

#[test]
fn unauthorized_shape_returns_404() {
    let (_tmp, app) = project_todos_app();
    let (status, body) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=-1",
        &json!({"projects": ["p2"]}),
    );
    assert_eq!(status, 404);
    assert_eq!(body, json!({"error": "shape_not_found"}));
}

#[test]
fn log_id_mismatch_returns_409() {
    let (_tmp, app) = project_todos_app();
    let (_, snap) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=-1",
        &json!({"projects": ["p1"]}),
    );
    let q = format!(
        "offset={}&log_id=00000000000000000000000000000000",
        snap["offset"].as_i64().unwrap()
    );
    let (status, body) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        &q,
        &json!({"projects": ["p1"]}),
    );
    assert_eq!(status, 409);
    assert_eq!(body, json!({"error": "resync_required"}));
}

#[test]
fn compact_makes_old_offsets_resync() {
    let (_tmp, app) = project_todos_app();
    let (_, snap) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=-1",
        &json!({"projects": ["p1"]}),
    );
    app.execute(
        "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
        &[Value::Integer(50), Value::Text("p1".into()), Value::Text("x".into())],
    )
    .unwrap();
    let stats = app.compact("todos", 0).unwrap();
    assert!(stats.retained_offset >= snap["offset"].as_i64().unwrap());

    let q = format!(
        "offset={}&log_id={}&shape_handle={}",
        snap["offset"].as_i64().unwrap(),
        snap["log_id"].as_str().unwrap(),
        snap["shape_handle"].as_str().unwrap(),
    );
    let (status, body) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        &q,
        &json!({"projects": ["p1"]}),
    );
    assert_eq!(status, 409);
    assert_eq!(body, json!({"error": "resync_required"}));
}

// F8: replay/live requests must carry a log_id. A missing log_id is treated the
// same as a mismatched one and forces a resync.
#[test]
fn log_id_missing_on_replay_returns_409() {
    let (_tmp, app) = project_todos_app();
    let (status, body) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=0",
        &json!({"projects": ["p1"]}),
    );
    assert_eq!(status, 409);
    assert_eq!(body, json!({"error": "resync_required"}));
}

// F1: compacting a quiet table (todos) must not be dragged forward by another
// busy table (events). With fewer than keep_last rows, nothing is deleted and the
// retained offset stays 0 — even though the global max(seq) is far higher.
#[test]
fn compact_quiet_table_keeps_its_log() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute_batch(
        r#"
        CREATE TABLE todos (
          id INTEGER PRIMARY KEY,
          title TEXT NOT NULL
        );
        CREATE TABLE events (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL
        );
        "#,
    )
    .unwrap();
    app.install_triggers("todos").unwrap();
    app.install_triggers("events").unwrap();

    // A few todos rows.
    for i in 1..=3 {
        app.execute(
            "INSERT INTO todos (id, title) VALUES (?, ?)",
            &[Value::Integer(i), Value::Text(format!("todo {i}"))],
        )
        .unwrap();
    }
    // MANY events rows so the global max(seq) is much higher than todos' max.
    for i in 1..=500 {
        app.execute(
            "INSERT INTO events (id, name) VALUES (?, ?)",
            &[Value::Integer(i), Value::Text(format!("ev {i}"))],
        )
        .unwrap();
    }

    let stats = app.compact("todos", 1000).unwrap();
    assert_eq!(stats.deleted_rows, 0, "quiet table log should be untouched");
    assert_eq!(stats.retained_offset, 0, "must not jump to global high-water");

    // todos log rows are still present — a repeat compact deletes nothing.
    let remaining = app.compact("todos", 1000).unwrap();
    assert_eq!(remaining.deleted_rows, 0);
    assert_eq!(remaining.retained_offset, 0);
}

// F1: the retained offset must never regress. After compacting at keep_last=0
// (retain everything up to todos' max), a later wide compact must not lower it.
#[test]
fn compact_retained_offset_is_monotonic() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute_batch(
        r#"
        CREATE TABLE todos (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
        "#,
    )
    .unwrap();
    app.install_triggers("todos").unwrap();
    for i in 1..=5 {
        app.execute(
            "INSERT INTO todos (id, title) VALUES (?, ?)",
            &[Value::Integer(i), Value::Text(format!("todo {i}"))],
        )
        .unwrap();
    }

    let high = app.compact("todos", 0).unwrap().retained_offset;
    assert!(high > 0, "retained offset should advance to todos max");

    // A wide window would compute candidate 0, but monotonicity must hold.
    let later = app.compact("todos", 1000).unwrap();
    assert_eq!(
        later.retained_offset, high,
        "retained offset must not regress below the previous value"
    );
    assert_eq!(later.deleted_rows, 0, "nothing new to delete");
}

#[test]
fn install_triggers_requires_primary_key() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute_batch("CREATE TABLE no_pk (a INTEGER, b INTEGER);")
        .unwrap();
    let err = app.install_triggers("no_pk").unwrap_err();
    match err {
        electrolite::Error::Bad(_) => {}
        other => panic!("expected Bad error, got {:?}", other),
    }
}

#[test]
fn composite_primary_keys_exposed_and_used_in_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let mut app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute_batch(
        r#"
        CREATE TABLE memberships (
          org TEXT NOT NULL,
          user TEXT NOT NULL,
          role TEXT NOT NULL,
          PRIMARY KEY (org, user)
        );
        INSERT INTO memberships (org, user, role) VALUES ('o1', 'u1', 'admin');
        "#,
    )
    .unwrap();
    app.install_triggers("memberships").unwrap();
    app.add_shape(
        "memberships",
        ShapeDef::new(
            "memberships",
            vec!["org".into(), "user".into(), "role".into()],
        ),
    );

    let (_, snap) = app.handle("/electrolite/v1/memberships", "offset=-1", &json!({}));
    assert_eq!(snap["key_columns"], json!(["org", "user"]));
    assert_eq!(
        snap["rows"],
        json!([{"org": "o1", "user": "u1", "role": "admin"}])
    );

    app.execute(
        "DELETE FROM memberships WHERE org = ? AND user = ?",
        &[Value::Text("o1".into()), Value::Text("u1".into())],
    )
    .unwrap();
    let q = format!(
        "offset={}&log_id={}&shape_handle={}",
        snap["offset"].as_i64().unwrap(),
        snap["log_id"].as_str().unwrap(),
        snap["shape_handle"].as_str().unwrap(),
    );
    let (_, body) = app.handle("/electrolite/v1/memberships", &q, &json!({}));
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages[0]["type"], "delete");
    assert_eq!(messages[0]["key"], json!({"org": "o1", "user": "u1"}));
}

#[test]
fn live_wait_wakes_when_write_commits() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let mut app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.live_timeout = Duration::from_millis(500);
    app.execute_batch(
        r#"
        CREATE TABLE todos (id INTEGER PRIMARY KEY, project_id TEXT NOT NULL);
        INSERT INTO todos (id, project_id) VALUES (1, 'p1');
        "#,
    )
    .unwrap();
    app.install_triggers("todos").unwrap();
    app.add_shape(
        "projectTodos",
        ShapeDef::new("todos", vec!["id".into(), "project_id".into()])
            .params(vec!["project_id".into()])
            .where_fn(|ctx| eq("project_id", json!(ctx.params["project_id"].clone()))),
    );

    let app = Arc::new(app);
    let (_, snap) = app.handle(
        "/electrolite/v1/projectTodos/p1",
        "offset=-1",
        &json!({}),
    );
    let writer = Arc::clone(&app);
    let h = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        writer
            .execute(
                "INSERT INTO todos (id, project_id) VALUES (?, ?)",
                &[Value::Integer(99), Value::Text("p1".into())],
            )
            .unwrap();
    });

    let q = format!(
        "offset={}&live=true&log_id={}&shape_handle={}",
        snap["offset"].as_i64().unwrap(),
        snap["log_id"].as_str().unwrap(),
        snap["shape_handle"].as_str().unwrap(),
    );
    let (status, body) = app.handle("/electrolite/v1/projectTodos/p1", &q, &json!({}));
    h.join().unwrap();

    assert_eq!(status, 200);
    let messages = body["messages"].as_array().unwrap();
    assert!(!messages.is_empty());
    assert_eq!(messages[0]["type"], "insert");
}

#[test]
fn log_id_is_collision_free_across_rapid_opens() {
    // 1000 fresh databases opened back-to-back must produce 1000
    // distinct log_id values. The xorshift+wall-clock RNG used to
    // generate identical IDs when system time didn't advance between
    // opens; getrandom closes that.
    use std::collections::HashSet;
    let mut ids = HashSet::new();
    for i in 0..1000 {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(format!("app-{i}.db"));
        let app = Electrolite::open(path.to_str().unwrap()).unwrap();
        // We can't directly call log_id_unsafe from here, so go via
        // snapshot of an empty trigger-less table — bootstrap already
        // populated log_id; snapshot's response carries it.
        app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .unwrap();
        app.install_triggers("t").unwrap();
        let shape = Shape::new("t", vec!["id".into()], all());
        let snap = app.snapshot(&shape).unwrap();
        assert!(
            ids.insert(snap.log_id.clone()),
            "duplicate log_id at iteration {i}: {}",
            snap.log_id
        );
    }
}

#[test]
fn batch_cap_signals_more_to_come() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
        .unwrap();
    app.install_triggers("t").unwrap();

    // Single batch of 25 rows; replay limit 1 → cap = 10 → first
    // page has 11 rows and up_to_date=false.
    let stmts: Vec<(&str, Vec<Value>)> = (1..=25)
        .map(|i| {
            (
                "INSERT INTO t (id, v) VALUES (?, ?)",
                vec![Value::Integer(i), Value::Text(format!("row-{i}"))],
            )
        })
        .collect();
    app.write_batch(&stmts).unwrap();

    let shape = Shape::new("t", vec!["id".into(), "v".into()], all());
    let r = app.replay(&shape, 0, 1).unwrap();
    assert_eq!(r.messages.len(), 11);
    assert!(!r.up_to_date);
}

#[test]
fn stale_current_batch_id_cleared_on_bootstrap() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    {
        let app = Electrolite::open(path.to_str().unwrap()).unwrap();
        app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .unwrap();
        app.install_triggers("t").unwrap();
        // Plant a stale batch_id and drop the engine without clearing.
        app.execute(
            "INSERT INTO _electrolite_meta (key, value) VALUES ('current_batch_id', 'STALE') \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            &[],
        )
        .unwrap();
    }
    let app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.execute(
        "INSERT INTO t (id, v) VALUES (?, ?)",
        &[Value::Integer(1), Value::Text("hello".into())],
    )
    .unwrap();
    let shape = Shape::new("t", vec!["id".into(), "v".into()], all());
    let r = app.replay(&shape, 0, 100).unwrap();
    assert_eq!(r.messages.len(), 1);
    let batch_id = r.messages[0]["batch_id"].as_str().unwrap();
    assert_ne!(batch_id, "STALE");
    assert_eq!(batch_id.len(), 32);
}

#[test]
fn shutdown_wakes_live_waiters_with_clean_response() {
    use std::time::Instant;

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("app.db");
    let mut app = Electrolite::open(path.to_str().unwrap()).unwrap();
    app.live_timeout = Duration::from_millis(10_000);
    app.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
        .unwrap();
    app.install_triggers("t").unwrap();
    app.add_shape(
        "all",
        ShapeDef::new("t", vec!["id".into(), "v".into()]),
    );

    let app = Arc::new(app);
    let (_, snap) = app.handle("/electrolite/v1/all", "offset=-1", &json!({}));

    let waiter = Arc::clone(&app);
    let q = format!(
        "offset={}&live=true&log_id={}&shape_handle={}",
        snap["offset"].as_i64().unwrap(),
        snap["log_id"].as_str().unwrap(),
        snap["shape_handle"].as_str().unwrap()
    );
    let h = thread::spawn(move || waiter.handle("/electrolite/v1/all", &q, &json!({})));

    thread::sleep(Duration::from_millis(50));
    let started = Instant::now();
    app.shutdown();
    let (status, body) = h.join().unwrap();
    let elapsed = started.elapsed();

    assert_eq!(status, 200);
    assert_eq!(body["shutdown"], true);
    assert_eq!(body["messages"], json!([]));
    assert!(
        elapsed < Duration::from_secs(1),
        "shutdown should wake within 1s, took {elapsed:?}"
    );
}

// Smoke test: keep direct API working alongside handle().
#[test]
fn direct_snapshot_replay_still_works() {
    let (_tmp, app) = open_app();
    let shape = Shape::new(
        "todos",
        vec!["id".into(), "project_id".into()],
        Predicate::Range {
            op: RangeOp::Lt,
            column: "id".into(),
            value: json!(3),
        },
    );
    let snap = app.snapshot(&shape).unwrap();
    assert_eq!(snap.rows.len(), 2);
    let _ = all();
    let _ = lt("id", json!(3));
    let _: Json = json!({});
}
