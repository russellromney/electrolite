use axum::Router;
use axum::routing::post;
use electrolite_core::ShapeRegistry;
use electrolite_server::{
    ServerState, TRUSTED_SHAPE_FACTORY_NAME, TrustedHeaderAuthorizer, TrustedHeaderShapeFactory,
    router,
};
use http::StatusCode;
use rusqlite::Connection;
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("typescript-e2e.db");
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
    conn.execute_batch(
        "
        INSERT INTO todos (id, project_id, title, done) VALUES
          (1, 'p1', 'ship electrolite', 0),
          (2, 'p2', 'other project', 0);
        ",
    )
    .expect("seed rows");

    let state = ServerState::new(path.clone(), ShapeRegistry::new(), TrustedHeaderAuthorizer)
        .with_shape_factory(TRUSTED_SHAPE_FACTORY_NAME, TrustedHeaderShapeFactory)
        .with_live_timeout(Duration::from_secs(5))
        .with_poll_interval(Duration::from_millis(10));
    let app = router(state.clone()).merge(test_routes(path, state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    println!("ELECTROLITE_ORIGIN=http://{addr}");
    axum::serve(listener, app).await.expect("serve");
}

fn test_routes(path: PathBuf, state: ServerState) -> Router {
    let insert_path = path.clone();
    let insert_state = state.clone();
    let insert_p1 = move || {
        let path = insert_path.clone();
        let state = insert_state.clone();
        async move {
            let conn = Connection::open(path).expect("open writer");
            conn.execute(
                "INSERT INTO todos (id, project_id, title, done) VALUES (3, 'p1', 'from ts backend', 0)",
                [],
            )
            .expect("insert p1 todo");
            state.notify_changed();
            StatusCode::NO_CONTENT
        }
    };

    let update_path = path.clone();
    let update_state = state.clone();
    let update_p2 = move || {
        let path = update_path.clone();
        let state = update_state.clone();
        async move {
            let conn = Connection::open(path).expect("open writer");
            conn.execute("UPDATE todos SET title='not visible' WHERE id=2", [])
                .expect("update p2 todo");
            state.notify_changed();
            StatusCode::NO_CONTENT
        }
    };

    Router::new()
        .route("/test/insert-p1", post(insert_p1))
        .route("/test/update-p2", post(update_p2))
}
