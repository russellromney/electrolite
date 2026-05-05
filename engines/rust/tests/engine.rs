use electrolite::{Electrolite, Predicate, Shape};
use rusqlite::types::Value;
use serde_json::json;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn setup() -> (tempfile::TempDir, Electrolite) {
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
          (2, 'p2', 'other project', 0);
        "#,
    )
    .unwrap();
    app.install_triggers("todos").unwrap();
    (tmp, app)
}

fn p1_shape() -> Shape {
    Shape {
        table: "todos".into(),
        columns: vec!["id".into(), "project_id".into(), "title".into(), "done".into()],
        predicate: Predicate::Eq {
            column: "project_id".into(),
            value: json!("p1"),
        },
    }
}

#[test]
fn snapshot_filters_by_predicate() {
    let (_tmp, app) = setup();
    let snap = app.snapshot(&p1_shape()).unwrap();
    assert_eq!(snap.key_columns, vec!["id".to_string()]);
    assert_eq!(snap.rows.len(), 1);
    assert_eq!(snap.rows[0]["title"], "ship electrolite");
    assert_eq!(snap.shape_handle.len(), 64);
}

#[test]
fn replay_insert_update_delete_and_batch() {
    let (_tmp, app) = setup();
    let snap = app.snapshot(&p1_shape()).unwrap();

    app.execute(
        "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
        &[Value::Integer(3), Value::Text("p1".into()), Value::Text("from rust".into())],
    )
    .unwrap();
    let r = app.replay(&p1_shape(), snap.offset, 100).unwrap();
    assert_eq!(r.messages.len(), 1);
    assert_eq!(r.messages[0]["type"], "insert");
    assert_eq!(r.messages[0]["value"]["title"], "from rust");

    app.execute(
        "UPDATE todos SET title = ? WHERE id = ?",
        &[Value::Text("renamed".into()), Value::Integer(3)],
    )
    .unwrap();
    let r2 = app.replay(&p1_shape(), r.offset, 100).unwrap();
    assert_eq!(r2.messages[0]["type"], "update");
    assert_eq!(r2.messages[0]["value"]["title"], "renamed");

    app.execute("DELETE FROM todos WHERE id = ?", &[Value::Integer(3)])
        .unwrap();
    let r3 = app.replay(&p1_shape(), r2.offset, 100).unwrap();
    assert_eq!(r3.messages[0]["type"], "delete");

    app.write_batch(&[
        (
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            vec![Value::Integer(4), Value::Text("p1".into()), Value::Text("a".into())],
        ),
        (
            "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
            vec![Value::Integer(5), Value::Text("p1".into()), Value::Text("b".into())],
        ),
    ])
    .unwrap();
    let r4 = app.replay(&p1_shape(), r3.offset, 100).unwrap();
    assert_eq!(r4.messages.len(), 2);
    let ids: Vec<&str> = r4
        .messages
        .iter()
        .map(|m| m["batch_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids[0], ids[1]);
}

#[test]
fn live_wait_wakes_on_change() {
    let (_tmp, mut app) = setup();
    app.live_timeout = Duration::from_millis(500);
    let app = Arc::new(app);

    let snap = app.snapshot(&p1_shape()).unwrap();
    let writer = Arc::clone(&app);
    let h = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        writer
            .execute(
                "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
                &[Value::Integer(9), Value::Text("p1".into()), Value::Text("live rust".into())],
            )
            .unwrap();
    });

    app.wait_for_change();
    let r = app.replay(&p1_shape(), snap.offset, 100).unwrap();
    h.join().unwrap();

    assert!(!r.messages.is_empty());
    assert_eq!(r.messages[0]["type"], "insert");
    assert_eq!(r.messages[0]["value"]["title"], "live rust");
}
