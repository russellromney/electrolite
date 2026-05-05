use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use electrolite_core::{Replay, Shape, ShapeMessage, ShapeRegistry, Snapshot};
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

#[derive(Clone)]
pub struct ServerState {
    db_path: Arc<PathBuf>,
    registry: Arc<ShapeRegistry>,
    authorizer: Arc<dyn Authorizer>,
    notify: Arc<Notify>,
    replay_limit: i64,
    live_timeout: Duration,
    poll_interval: Duration,
}

impl ServerState {
    pub fn new(
        db_path: impl Into<PathBuf>,
        registry: ShapeRegistry,
        authorizer: impl Authorizer,
    ) -> Self {
        Self {
            db_path: Arc::new(db_path.into()),
            registry: Arc::new(registry),
            authorizer: Arc::new(authorizer),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDecision {
    Allow,
    Deny,
}

pub trait Authorizer: Send + Sync + 'static {
    fn authorize(&self, context: &AuthContext<'_>) -> AuthDecision;
}

#[derive(Debug)]
pub struct AuthContext<'a> {
    pub headers: &'a HeaderMap,
    pub extensions: &'a http::Extensions,
    pub shape: &'a Shape,
    pub shape_name: &'a str,
    pub offset: i64,
    pub live: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct AllowAll;

impl Authorizer for AllowAll {
    fn authorize(&self, _context: &AuthContext<'_>) -> AuthDecision {
        AuthDecision::Allow
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
    Forbidden(String),
    Sqlite(rusqlite::Error),
    Electrolite(electrolite_sqlite::Error),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::ShapeNotFound(name) => {
                (StatusCode::NOT_FOUND, format!("shape not found: {name}"))
            }
            Self::Forbidden(name) => (StatusCode::FORBIDDEN, format!("shape forbidden: {name}")),
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
    parts: Parts,
    State(state): State<ServerState>,
    Path(name): Path<String>,
    Query(query): Query<ShapeQuery>,
) -> Result<Response, ServerError> {
    let shape = state
        .registry
        .get(&name)
        .cloned()
        .ok_or_else(|| ServerError::ShapeNotFound(name.clone()))?;
    authorize_shape(&state, &parts, &name, &shape, &query)?;
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

fn authorize_shape(
    state: &ServerState,
    parts: &Parts,
    name: &str,
    shape: &Shape,
    query: &ShapeQuery,
) -> Result<(), ServerError> {
    let context = AuthContext {
        headers: &parts.headers,
        extensions: &parts.extensions,
        shape,
        shape_name: name,
        offset: query.offset,
        live: query.live,
    };
    match state.authorizer.authorize(&context) {
        AuthDecision::Allow => Ok(()),
        AuthDecision::Deny => Err(ServerError::Forbidden(name.to_string())),
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
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    #[derive(Debug, Clone)]
    struct Session {
        scope: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedAuth {
        shape_name: String,
        auth_scope: String,
        header_scope: Option<String>,
        session_scope: Option<String>,
        offset: i64,
        live: bool,
    }

    #[derive(Debug)]
    struct DenyAll;

    impl Authorizer for DenyAll {
        fn authorize(&self, _context: &AuthContext<'_>) -> AuthDecision {
            AuthDecision::Deny
        }
    }

    #[derive(Debug)]
    struct ScopeAuthorizer {
        seen: Arc<Mutex<Vec<RecordedAuth>>>,
    }

    impl Authorizer for ScopeAuthorizer {
        fn authorize(&self, context: &AuthContext<'_>) -> AuthDecision {
            let header_scope = context
                .headers
                .get("x-electrolite-scope")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let session_scope = context
                .extensions
                .get::<Session>()
                .map(|session| session.scope.clone());
            self.seen.lock().unwrap().push(RecordedAuth {
                shape_name: context.shape_name.to_string(),
                auth_scope: context.shape.auth_scope.clone(),
                header_scope: header_scope.clone(),
                session_scope: session_scope.clone(),
                offset: context.offset,
                live: context.live,
            });

            if header_scope.as_deref() == Some(context.shape.auth_scope.as_str())
                && session_scope.as_deref() == Some(context.shape.auth_scope.as_str())
            {
                AuthDecision::Allow
            } else {
                AuthDecision::Deny
            }
        }
    }

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
        let state = ServerState::new(path.clone(), registry, AllowAll)
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
    async fn denied_shape_returns_403_before_opening_sqlite() {
        let mut registry = ShapeRegistry::new();
        registry.add(active_users_shape());
        let app = router(ServerState::new("/missing/app.db", registry, DenyAll));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/electrolite/v1/shape/activeUsers?offset=-1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn authorizer_receives_headers_extensions_shape_and_query() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let authorizer = ScopeAuthorizer { seen: seen.clone() };
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
        let mut registry = ShapeRegistry::new();
        registry.add(active_users_shape());
        let app = router(
            ServerState::new(path, registry, authorizer)
                .with_live_timeout(Duration::from_millis(1))
                .with_poll_interval(Duration::from_millis(1)),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/electrolite/v1/shape/activeUsers?offset=7&live=true")
                    .header("x-electrolite-scope", "public")
                    .extension(Session {
                        scope: "public".to_string(),
                    })
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![RecordedAuth {
                shape_name: "activeUsers".to_string(),
                auth_scope: "public".to_string(),
                header_scope: Some("public".to_string()),
                session_scope: Some("public".to_string()),
                offset: 7,
                live: true,
            }]
        );
    }

    #[tokio::test]
    async fn authorizer_denies_when_scope_does_not_match() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let authorizer = ScopeAuthorizer { seen };
        let mut registry = ShapeRegistry::new();
        registry.add(active_users_shape());
        let app = router(ServerState::new("/missing/app.db", registry, authorizer));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/electrolite/v1/shape/activeUsers?offset=-1")
                    .header("x-electrolite-scope", "private")
                    .extension(Session {
                        scope: "public".to_string(),
                    })
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
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
