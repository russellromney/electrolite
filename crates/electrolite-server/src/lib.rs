use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use electrolite_core::{Replay, ShapeMessage, ShapeRegistry, Snapshot};
use http::StatusCode;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until};

pub const DEFAULT_ROUTE_PREFIX: &str = "/electrolite/v1";

#[derive(Debug, Clone)]
pub struct ServerState {
    db_path: Arc<PathBuf>,
    registry: Arc<ShapeRegistry>,
    notify: Arc<Notify>,
    replay_limit: i64,
    live_timeout: Duration,
    poll_interval: Duration,
}

impl ServerState {
    pub fn new(db_path: impl Into<PathBuf>, registry: ShapeRegistry) -> Self {
        Self {
            db_path: Arc::new(db_path.into()),
            registry: Arc::new(registry),
            notify: Arc::new(Notify::new()),
            replay_limit: 1000,
            live_timeout: Duration::from_secs(20),
            poll_interval: Duration::from_millis(250),
        }
    }

    pub fn with_replay_limit(mut self, replay_limit: i64) -> Self {
        self.replay_limit = replay_limit.max(1);
        self
    }

    pub fn with_live_timeout(mut self, live_timeout: Duration) -> Self {
        self.live_timeout = live_timeout;
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn notify_changed(&self) {
        self.notify.notify_waiters();
    }
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/electrolite/v1/shape/{name}", get(get_shape))
        .with_state(state)
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShapeQuery {
    pub offset: i64,
    #[serde(default)]
    pub live: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ShapeResponse {
    Snapshot {
        rows: Vec<Value>,
        offset: i64,
    },
    Replay {
        messages: Vec<ShapeMessage>,
        offset: i64,
    },
}

impl From<Snapshot> for ShapeResponse {
    fn from(snapshot: Snapshot) -> Self {
        Self::Snapshot {
            rows: snapshot.rows,
            offset: snapshot.offset,
        }
    }
}

impl From<Replay> for ShapeResponse {
    fn from(replay: Replay) -> Self {
        Self::Replay {
            messages: replay.messages,
            offset: replay.offset,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

enum ServerError {
    ShapeNotFound(String),
    Sqlite(rusqlite::Error),
    Electrolite(electrolite_sqlite::Error),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::ShapeNotFound(name) => {
                (StatusCode::NOT_FOUND, format!("shape not found: {name}"))
            }
            Self::Sqlite(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            Self::Electrolite(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}

impl From<rusqlite::Error> for ServerError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}

impl From<electrolite_sqlite::Error> for ServerError {
    fn from(e: electrolite_sqlite::Error) -> Self {
        Self::Electrolite(e)
    }
}

async fn get_shape(
    State(state): State<ServerState>,
    Path(name): Path<String>,
    Query(query): Query<ShapeQuery>,
) -> Result<Response, ServerError> {
    let shape = state
        .registry
        .get(&name)
        .cloned()
        .ok_or_else(|| ServerError::ShapeNotFound(name.clone()))?;
    let conn = Connection::open(state.db_path.as_ref())?;

    if query.offset < 0 {
        let snapshot = electrolite_sqlite::initial_snapshot(&conn, &shape)?;
        Ok(Json(ShapeResponse::from(snapshot)).into_response())
    } else if query.live {
        drop(conn);
        live_replay(state, shape, query.offset).await
    } else {
        let replay = electrolite_sqlite::replay(&conn, &shape, query.offset, state.replay_limit)?;
        Ok(Json(ShapeResponse::from(replay)).into_response())
    }
}

async fn live_replay(
    state: ServerState,
    shape: electrolite_core::Shape,
    offset: i64,
) -> Result<Response, ServerError> {
    let deadline = Instant::now() + state.live_timeout;

    loop {
        let conn = Connection::open(state.db_path.as_ref())?;
        let replay = electrolite_sqlite::replay(&conn, &shape, offset, state.replay_limit)?;
        if replay.offset > offset || !replay.messages.is_empty() {
            return Ok(Json(ShapeResponse::from(replay)).into_response());
        }

        if Instant::now() >= deadline {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }

        let next_poll = Instant::now() + state.poll_interval;
        tokio::select! {
            _ = state.notify.notified() => {}
            _ = sleep_until(next_poll.min(deadline)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use electrolite_core::{Predicate, Shape};
    use http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    fn active_users_shape() -> Shape {
        Shape {
            name: "activeUsers".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "active".to_string()],
            predicate: Predicate::Eq {
                column: "active".to_string(),
                value: json!(1),
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        }
    }

    fn setup() -> (tempfile::TempDir, PathBuf, ServerState, Router) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              active INTEGER NOT NULL DEFAULT 0
            );
            ",
        )
        .unwrap();
        electrolite_sqlite::install_triggers(&conn, "users", "id").unwrap();
        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (2, 'Grace', 0)",
            [],
        )
        .unwrap();

        let mut registry = ShapeRegistry::new();
        registry.add(active_users_shape());
        let state = ServerState::new(path.clone(), registry)
            .with_live_timeout(Duration::from_millis(500))
            .with_poll_interval(Duration::from_millis(25));
        let app = router(state.clone());
        (dir, path, state, app)
    }

    async fn json_response(app: Router, uri: &str) -> (StatusCode, ShapeResponse) {
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed = serde_json::from_slice::<ShapeResponse>(&body).unwrap();
        (status, parsed)
    }

    #[tokio::test]
    async fn serves_initial_snapshot() {
        let (_dir, _path, _state, app) = setup();

        let (status, response) =
            json_response(app, "/electrolite/v1/shape/activeUsers?offset=-1").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            ShapeResponse::Snapshot {
                rows: vec![json!({"id": 1, "name": "Ada", "active": 1})],
                offset: 2,
            }
        );
    }

    #[tokio::test]
    async fn serves_replay_after_offset() {
        let (_dir, path, _state, app) = setup();
        let conn = Connection::open(path).unwrap();
        conn.execute("UPDATE users SET active=1 WHERE id=2", [])
            .unwrap();

        let (status, response) =
            json_response(app, "/electrolite/v1/shape/activeUsers?offset=2").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            ShapeResponse::Replay {
                messages: vec![ShapeMessage::Insert {
                    key: json!({"id": 2}),
                    value: json!({"id": 2, "name": "Grace", "active": 1}),
                    offset: 3,
                }],
                offset: 3,
            }
        );
    }

    #[tokio::test]
    async fn missing_shape_returns_404() {
        let (_dir, _path, _state, app) = setup();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/electrolite/v1/shape/nope?offset=-1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn live_request_waits_until_shape_changes() {
        let (_dir, path, state, app) = setup();
        let live = tokio::spawn(async move {
            json_response(app, "/electrolite/v1/shape/activeUsers?offset=2&live=true").await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let conn = Connection::open(path).unwrap();
        conn.execute("UPDATE users SET active=1 WHERE id=2", [])
            .unwrap();
        state.notify_changed();

        let (status, response) = live.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response,
            ShapeResponse::Replay {
                messages: vec![ShapeMessage::Insert {
                    key: json!({"id": 2}),
                    value: json!({"id": 2, "name": "Grace", "active": 1}),
                    offset: 3,
                }],
                offset: 3,
            }
        );
    }

    #[tokio::test]
    async fn live_request_times_out_with_no_content() {
        let (_dir, _path, _state, app) = setup();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/electrolite/v1/shape/activeUsers?offset=2&live=true")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn electric_like_http_scenario_from_backend_to_client() {
        let (_dir, path, state, app) = setup();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let base = format!("http://{addr}/electrolite/v1/shape/activeUsers");

        let snapshot = client
            .get(&base)
            .query(&[("offset", "-1")])
            .send()
            .await
            .unwrap()
            .json::<ShapeResponse>()
            .await
            .unwrap();
        assert_eq!(
            snapshot,
            ShapeResponse::Snapshot {
                rows: vec![json!({"id": 1, "name": "Ada", "active": 1})],
                offset: 2,
            }
        );

        let live = {
            let client = client.clone();
            let base = base.clone();
            tokio::spawn(async move {
                client
                    .get(base)
                    .query(&[("offset", "2"), ("live", "true")])
                    .send()
                    .await
                    .unwrap()
                    .json::<ShapeResponse>()
                    .await
                    .unwrap()
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        let conn = Connection::open(path).unwrap();
        conn.execute("UPDATE users SET active=1 WHERE id=2", [])
            .unwrap();
        state.notify_changed();

        assert_eq!(
            live.await.unwrap(),
            ShapeResponse::Replay {
                messages: vec![ShapeMessage::Insert {
                    key: json!({"id": 2}),
                    value: json!({"id": 2, "name": "Grace", "active": 1}),
                    offset: 3,
                }],
                offset: 3,
            }
        );
        server.abort();
    }
}
