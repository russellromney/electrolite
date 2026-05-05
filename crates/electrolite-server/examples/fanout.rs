use electrolite_core::{Predicate, Shape, ShapeRegistry};
use electrolite_server::{AllowAll, ServerState, ShapeResponse, router};
use rusqlite::Connection;
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let rows = env_usize("ELECTROLITE_BENCH_ROWS", 1_000);
    let clients = env_usize("ELECTROLITE_BENCH_CLIENTS", 100);
    let pool_size = env_usize("ELECTROLITE_BENCH_POOL", 4);
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("bench.db");
    let conn = Connection::open(&path).expect("open sqlite");
    conn.execute_batch(
        "
        CREATE TABLE todos (
          id INTEGER PRIMARY KEY,
          project_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL DEFAULT 0
        );
        ",
    )
    .expect("create schema");
    electrolite_sqlite::install_triggers(&conn, "todos", "id").expect("install triggers");
    let tx = conn.unchecked_transaction().expect("start seed tx");
    for id in 1..=rows {
        tx.execute(
            "INSERT INTO todos (id, project_id, title, done) VALUES (?1, 'p1', ?2, 0)",
            (id as i64, format!("todo {id}")),
        )
        .expect("insert seed row");
    }
    tx.commit().expect("commit seed");

    let mut registry = ShapeRegistry::new();
    registry.add(Shape {
        name: "openTodos".to_string(),
        table: "todos".to_string(),
        columns: vec![
            "id".to_string(),
            "project_id".to_string(),
            "title".to_string(),
            "done".to_string(),
        ],
        predicate: Predicate::Eq {
            column: "done".to_string(),
            value: json!(0),
        },
        auth_scope: "bench".to_string(),
        schema_version: 1,
    });

    let state = ServerState::new(path.clone(), registry, AllowAll)
        .with_connection_pool_size(pool_size)
        .with_live_timeout(Duration::from_secs(5))
        .with_poll_interval(Duration::from_millis(5));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let app = router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let client = reqwest::Client::new();
    let base = format!("http://{addr}/electrolite/v1/shape/openTodos");

    let started = Instant::now();
    let snapshot = client
        .get(&base)
        .query(&[("offset", "-1")])
        .send()
        .await
        .expect("snapshot response")
        .json::<ShapeResponse>()
        .await
        .expect("snapshot json");
    let snapshot_elapsed = started.elapsed();
    let ShapeResponse::Snapshot { offset, rows, .. } = snapshot else {
        panic!("expected snapshot");
    };

    let mut live = Vec::new();
    let live_started = Instant::now();
    for _ in 0..clients {
        let client = client.clone();
        let base = base.clone();
        live.push(tokio::spawn(async move {
            client
                .get(base)
                .query(&[("offset", offset.to_string()), ("live", "true".to_string())])
                .send()
                .await
                .expect("live response")
                .json::<ShapeResponse>()
                .await
                .expect("live json")
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    let conn = Connection::open(&path).expect("open writer");
    conn.execute("UPDATE todos SET title='changed' WHERE id=1", [])
        .expect("write update");
    state.notify_changed();

    for task in live {
        let ShapeResponse::Replay { messages, .. } = task.await.expect("join live") else {
            panic!("expected replay");
        };
        assert_eq!(messages.len(), 1);
    }
    let live_elapsed = live_started.elapsed();

    println!("rows={rows}", rows = rows.len());
    println!("clients={clients}");
    println!("pool_size={pool_size}");
    println!("snapshot_ms={}", snapshot_elapsed.as_millis());
    println!("live_fanout_ms={}", live_elapsed.as_millis());
    server.abort();
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
