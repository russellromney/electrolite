use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use electrolite_core::{Predicate, Replay, Shape, ShapeMessage, ShapeRegistry, Snapshot};
use http::StatusCode;
use rusqlite::Connection;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, watch};
use tokio::time::{Instant, sleep_until};

pub const DEFAULT_ROUTE_PREFIX: &str = "/electrolite/v1";
pub const TRUSTED_SHAPE_FACTORY_NAME: &str = "trusted";

#[derive(Clone)]
pub struct ServerState {
    pool: ConnectionPool,
    registry: Arc<ShapeRegistry>,
    factories: Arc<HashMap<String, Arc<dyn ShapeFactory>>>,
    authorizer: Arc<dyn Authorizer>,
    notify: Arc<Notify>,
    waiters: LiveWaiters,
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
        let db_path = db_path.into();
        Self {
            pool: ConnectionPool::new(db_path, 1),
            registry: Arc::new(registry),
            factories: Arc::new(HashMap::new()),
            authorizer: Arc::new(authorizer),
            notify: Arc::new(Notify::new()),
            waiters: LiveWaiters::default(),
            replay_limit: 1000,
            live_timeout: Duration::from_secs(20),
            poll_interval: Duration::from_millis(250),
        }
    }

    pub fn with_replay_limit(mut self, replay_limit: i64) -> Self {
        self.replay_limit = replay_limit.max(1);
        self
    }

    pub fn with_connection_pool_size(mut self, pool_size: usize) -> Self {
        self.pool = ConnectionPool::new(self.pool.db_path.as_ref().clone(), pool_size.max(1));
        self
    }

    pub fn with_shape_factory(
        mut self,
        name: impl Into<String>,
        factory: impl ShapeFactory,
    ) -> Self {
        let mut factories = self.factories.as_ref().clone();
        factories.insert(name.into(), Arc::new(factory));
        self.factories = Arc::new(factories);
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

    pub async fn write<T>(
        &self,
        write: impl FnOnce(&Connection) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let conn = self.pool.get().await?;
        let out = write(&conn)?;
        drop(conn);
        self.notify_changed();
        Ok(out)
    }

    pub async fn write_batch<T>(
        &self,
        write: impl FnOnce(&rusqlite::Transaction<'_>) -> electrolite_sqlite::Result<T>,
    ) -> electrolite_sqlite::Result<T> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(electrolite_sqlite::Error::from)?;
        let out = electrolite_sqlite::change_batch(&mut conn, write)?;
        drop(conn);
        self.notify_changed();
        Ok(out)
    }

    pub async fn compact_log_to_last(
        &self,
        keep_last: i64,
    ) -> electrolite_sqlite::Result<electrolite_sqlite::RetentionStats> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(electrolite_sqlite::Error::from)?;
        let stats = electrolite_sqlite::compact_log_to_last(&conn, keep_last)?;
        drop(conn);
        self.notify_changed();
        Ok(stats)
    }

    pub async fn compact_log_to_last_for_table(
        &self,
        table_name: &str,
        keep_last: i64,
    ) -> electrolite_sqlite::Result<electrolite_sqlite::RetentionStats> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(electrolite_sqlite::Error::from)?;
        let stats =
            electrolite_sqlite::compact_log_to_last_for_table(&conn, table_name, keep_last)?;
        drop(conn);
        self.notify_changed();
        Ok(stats)
    }
}

#[derive(Clone)]
struct ConnectionPool {
    db_path: Arc<PathBuf>,
    idle: Arc<Mutex<Vec<Connection>>>,
    permits: Arc<Semaphore>,
}

impl ConnectionPool {
    fn new(db_path: impl Into<PathBuf>, size: usize) -> Self {
        Self {
            db_path: Arc::new(db_path.into()),
            idle: Arc::new(Mutex::new(Vec::new())),
            permits: Arc::new(Semaphore::new(size.max(1))),
        }
    }

    async fn get(&self) -> Result<PooledConnection, rusqlite::Error> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("connection pool semaphore is never closed");
        let conn = self
            .idle
            .lock()
            .expect("connection pool mutex is not poisoned")
            .pop()
            .map(Ok)
            .unwrap_or_else(|| Connection::open(self.db_path.as_ref()))?;

        Ok(PooledConnection {
            conn: Some(conn),
            idle: self.idle.clone(),
            _permit: permit,
        })
    }
}

struct PooledConnection {
    conn: Option<Connection>,
    idle: Arc<Mutex<Vec<Connection>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("pooled connection is present")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("pooled connection is present")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.idle
                .lock()
                .expect("connection pool mutex is not poisoned")
                .push(conn);
        }
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

pub trait ShapeFactory: Send + Sync + 'static {
    fn build(&self, context: &ShapeFactoryContext<'_>) -> ShapeFactoryDecision;
}

#[derive(Debug)]
pub struct ShapeFactoryContext<'a> {
    pub headers: &'a HeaderMap,
    pub extensions: &'a http::Extensions,
    pub factory_name: &'a str,
    pub path: &'a str,
    pub offset: i64,
    pub live: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeFactoryDecision {
    Shape(Shape),
    Deny,
    BadRequest,
}

#[derive(Debug, Clone, Copy)]
pub struct TrustedHeaderShapeFactory;

impl ShapeFactory for TrustedHeaderShapeFactory {
    fn build(&self, context: &ShapeFactoryContext<'_>) -> ShapeFactoryDecision {
        let Some(name) = required_header(context.headers, "x-electrolite-shape-name") else {
            return ShapeFactoryDecision::BadRequest;
        };
        let Some(table) = required_header(context.headers, "x-electrolite-table") else {
            return ShapeFactoryDecision::BadRequest;
        };
        let Some(auth_scope) = required_header(context.headers, "x-electrolite-auth-scope") else {
            return ShapeFactoryDecision::BadRequest;
        };
        let Some(columns) =
            required_json_header::<Vec<String>>(context.headers, "x-electrolite-columns")
        else {
            return ShapeFactoryDecision::BadRequest;
        };
        let Some(predicate) =
            required_json_header::<Predicate>(context.headers, "x-electrolite-predicate")
        else {
            return ShapeFactoryDecision::BadRequest;
        };
        let Some(schema_version) = required_header(context.headers, "x-electrolite-schema-version")
            .and_then(|value| value.parse::<u64>().ok())
        else {
            return ShapeFactoryDecision::BadRequest;
        };

        if columns.is_empty() {
            return ShapeFactoryDecision::BadRequest;
        }

        ShapeFactoryDecision::Shape(Shape {
            name: name.to_string(),
            table: table.to_string(),
            columns,
            predicate,
            auth_scope: auth_scope.to_string(),
            schema_version,
        })
    }
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
}

fn required_json_header<T: DeserializeOwned>(headers: &HeaderMap, name: &str) -> Option<T> {
    required_header(headers, name).and_then(|value| serde_json::from_str(value).ok())
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

#[derive(Debug, Clone, Copy)]
pub struct TrustedHeaderAuthorizer;

impl Authorizer for TrustedHeaderAuthorizer {
    fn authorize(&self, context: &AuthContext<'_>) -> AuthDecision {
        if context
            .headers
            .get("x-electrolite-scope")
            .and_then(|value| value.to_str().ok())
            == Some(context.shape.auth_scope.as_str())
        {
            AuthDecision::Allow
        } else {
            AuthDecision::Deny
        }
    }
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/electrolite/v1/shape/{name}", get(get_shape))
        .route(
            "/electrolite/v1/factory/{name}",
            get(get_factory_shape_root),
        )
        .route(
            "/electrolite/v1/factory/{name}/{*path}",
            get(get_factory_shape_path),
        )
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
        key_columns: Vec<String>,
        rows: Vec<Value>,
        offset: i64,
        up_to_date: bool,
    },
    Replay {
        messages: Vec<ShapeMessage>,
        offset: i64,
        up_to_date: bool,
    },
}

impl From<Snapshot> for ShapeResponse {
    fn from(snapshot: Snapshot) -> Self {
        Self::Snapshot {
            key_columns: snapshot.key_columns,
            rows: snapshot.rows,
            offset: snapshot.offset,
            up_to_date: snapshot.up_to_date,
        }
    }
}

impl From<Replay> for ShapeResponse {
    fn from(replay: Replay) -> Self {
        Self::Replay {
            messages: replay.messages,
            offset: replay.offset,
            up_to_date: replay.up_to_date,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

enum ServerError {
    ShapeNotFound,
    ShapeDenied,
    BadShapeRequest,
    ResyncRequired,
    Sqlite,
    Electrolite,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::ShapeNotFound | Self::ShapeDenied => {
                (StatusCode::NOT_FOUND, "shape_not_found".to_string())
            }
            Self::BadShapeRequest => (StatusCode::BAD_REQUEST, "bad_shape_request".to_string()),
            Self::ResyncRequired => (StatusCode::CONFLICT, "resync_required".to_string()),
            Self::Sqlite | Self::Electrolite => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_server_error".to_string(),
            ),
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}

impl From<rusqlite::Error> for ServerError {
    fn from(_e: rusqlite::Error) -> Self {
        Self::Sqlite
    }
}

impl From<electrolite_sqlite::Error> for ServerError {
    fn from(e: electrolite_sqlite::Error) -> Self {
        match e {
            electrolite_sqlite::Error::ResyncRequired { .. } => Self::ResyncRequired,
            _ => Self::Electrolite,
        }
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
        .ok_or(ServerError::ShapeNotFound)?;
    serve_shape(parts, state, name, shape, query).await
}

async fn get_factory_shape_root(
    parts: Parts,
    State(state): State<ServerState>,
    Path(name): Path<String>,
    Query(query): Query<ShapeQuery>,
) -> Result<Response, ServerError> {
    get_factory_shape(parts, state, name, String::new(), query).await
}

async fn get_factory_shape_path(
    parts: Parts,
    State(state): State<ServerState>,
    Path((name, path)): Path<(String, String)>,
    Query(query): Query<ShapeQuery>,
) -> Result<Response, ServerError> {
    get_factory_shape(parts, state, name, path, query).await
}

async fn get_factory_shape(
    parts: Parts,
    state: ServerState,
    name: String,
    path: String,
    query: ShapeQuery,
) -> Result<Response, ServerError> {
    let factory = state
        .factories
        .get(&name)
        .cloned()
        .ok_or(ServerError::ShapeNotFound)?;
    let context = ShapeFactoryContext {
        headers: &parts.headers,
        extensions: &parts.extensions,
        factory_name: &name,
        path: &path,
        offset: query.offset,
        live: query.live,
    };
    let shape = match factory.build(&context) {
        ShapeFactoryDecision::Shape(shape) => shape,
        ShapeFactoryDecision::Deny => return Err(ServerError::ShapeDenied),
        ShapeFactoryDecision::BadRequest => return Err(ServerError::BadShapeRequest),
    };
    let shape_name = shape.name.clone();
    serve_shape(parts, state, shape_name, shape, query).await
}

async fn serve_shape(
    parts: Parts,
    state: ServerState,
    name: String,
    shape: Shape,
    query: ShapeQuery,
) -> Result<Response, ServerError> {
    authorize_shape(&state, &parts, &name, &shape, &query)?;
    let conn = state.pool.get().await?;

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
        AuthDecision::Deny => Err(ServerError::ShapeDenied),
    }
}

async fn live_replay(
    state: ServerState,
    shape: electrolite_core::Shape,
    offset: i64,
) -> Result<Response, ServerError> {
    let conn = state.pool.get().await?;
    let shape_handle = electrolite_sqlite::shape_handle(&conn, &shape)?;
    drop(conn);
    let key = LiveWaitKey {
        shape_handle,
        offset,
    };
    match state.waiters.subscribe_or_create(key.clone()) {
        WaitSubscription::Leader { sender } => {
            let result = run_live_replay(state.clone(), shape, offset).await;
            state.waiters.finish(&key, sender, result.clone());
            live_result_response(result)
        }
        WaitSubscription::Follower { receiver } => await_live_result(receiver).await,
    }
}

async fn run_live_replay(
    state: ServerState,
    shape: electrolite_core::Shape,
    offset: i64,
) -> LiveResult {
    let deadline = Instant::now() + state.live_timeout;

    loop {
        let conn = match state.pool.get().await {
            Ok(conn) => conn,
            Err(_) => return LiveResult::InternalError,
        };
        let replay = match electrolite_sqlite::replay(&conn, &shape, offset, state.replay_limit) {
            Ok(replay) => replay,
            Err(electrolite_sqlite::Error::ResyncRequired { .. }) => {
                return LiveResult::ResyncRequired;
            }
            Err(_) => return LiveResult::InternalError,
        };
        if replay.offset > offset || !replay.messages.is_empty() {
            return LiveResult::Replay(replay);
        }

        if Instant::now() >= deadline {
            return LiveResult::NoContent;
        }

        let next_poll = Instant::now() + state.poll_interval;
        tokio::select! {
            _ = state.notify.notified() => {}
            _ = sleep_until(next_poll.min(deadline)) => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LiveWaitKey {
    shape_handle: String,
    offset: i64,
}

#[derive(Clone, Default)]
struct LiveWaiters {
    inner: Arc<Mutex<HashMap<LiveWaitKey, watch::Receiver<Option<LiveResult>>>>>,
}

enum WaitSubscription {
    Leader {
        sender: watch::Sender<Option<LiveResult>>,
    },
    Follower {
        receiver: watch::Receiver<Option<LiveResult>>,
    },
}

impl LiveWaiters {
    fn subscribe_or_create(&self, key: LiveWaitKey) -> WaitSubscription {
        let mut inner = self
            .inner
            .lock()
            .expect("live waiter mutex is not poisoned");
        if let Some(receiver) = inner.get(&key) {
            return WaitSubscription::Follower {
                receiver: receiver.clone(),
            };
        }

        let (sender, receiver) = watch::channel(None);
        inner.insert(key, receiver.clone());
        WaitSubscription::Leader { sender }
    }

    fn finish(
        &self,
        key: &LiveWaitKey,
        sender: watch::Sender<Option<LiveResult>>,
        result: LiveResult,
    ) {
        let _ = sender.send(Some(result));
        self.inner
            .lock()
            .expect("live waiter mutex is not poisoned")
            .remove(key);
    }
}

#[derive(Debug, Clone)]
enum LiveResult {
    Replay(Replay),
    NoContent,
    ResyncRequired,
    InternalError,
}

async fn await_live_result(
    mut receiver: watch::Receiver<Option<LiveResult>>,
) -> Result<Response, ServerError> {
    loop {
        if let Some(result) = receiver.borrow().clone() {
            return live_result_response(result);
        }
        if receiver.changed().await.is_err() {
            return Err(ServerError::Electrolite);
        }
    }
}

fn live_result_response(result: LiveResult) -> Result<Response, ServerError> {
    match result {
        LiveResult::Replay(replay) => Ok(Json(ShapeResponse::from(replay)).into_response()),
        LiveResult::NoContent => Ok(StatusCode::NO_CONTENT.into_response()),
        LiveResult::ResyncRequired => Err(ServerError::ResyncRequired),
        LiveResult::InternalError => Err(ServerError::Electrolite),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use electrolite_core::{Predicate, Shape};
    use http::{Request, StatusCode};
    use serde_json::json;
    use std::collections::BTreeMap;
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

    #[derive(Debug)]
    struct HeaderScopeAuthorizer;

    impl Authorizer for HeaderScopeAuthorizer {
        fn authorize(&self, context: &AuthContext<'_>) -> AuthDecision {
            if context
                .headers
                .get("x-electrolite-scope")
                .and_then(|value| value.to_str().ok())
                == Some(context.shape.auth_scope.as_str())
            {
                AuthDecision::Allow
            } else {
                AuthDecision::Deny
            }
        }
    }

    #[derive(Debug)]
    struct ProjectTodosFactory;

    impl ShapeFactory for ProjectTodosFactory {
        fn build(&self, context: &ShapeFactoryContext<'_>) -> ShapeFactoryDecision {
            let project_id = context.path.trim_matches('/');
            if project_id.is_empty() || project_id.contains('/') {
                return ShapeFactoryDecision::BadRequest;
            }
            if context
                .headers
                .get("x-electrolite-project")
                .and_then(|value| value.to_str().ok())
                != Some(project_id)
            {
                return ShapeFactoryDecision::Deny;
            }

            ShapeFactoryDecision::Shape(Shape {
                name: format!("projectTodos/{project_id}"),
                table: "todos".to_string(),
                columns: vec![
                    "id".to_string(),
                    "project_id".to_string(),
                    "title".to_string(),
                    "done".to_string(),
                ],
                predicate: Predicate::Eq {
                    column: "project_id".to_string(),
                    value: json!(project_id),
                },
                auth_scope: format!("project:{project_id}"),
                schema_version: 1,
            })
        }
    }

    #[derive(Debug)]
    struct MaterializedShape {
        key_columns: Vec<String>,
        offset: i64,
        rows: BTreeMap<String, Value>,
    }

    impl MaterializedShape {
        fn new(key_columns: &[&str]) -> Self {
            Self {
                key_columns: key_columns
                    .iter()
                    .map(|column| column.to_string())
                    .collect(),
                offset: -1,
                rows: BTreeMap::new(),
            }
        }

        fn apply(&mut self, response: ShapeResponse) {
            match response {
                ShapeResponse::Snapshot {
                    key_columns,
                    rows,
                    offset,
                    ..
                } => {
                    self.key_columns = key_columns;
                    self.rows.clear();
                    for row in rows {
                        self.rows.insert(self.key_for_row(&row), row);
                    }
                    self.offset = offset;
                }
                ShapeResponse::Replay {
                    messages, offset, ..
                } => {
                    for message in messages {
                        match message {
                            ShapeMessage::Insert { key, value, .. }
                            | ShapeMessage::Update { key, value, .. } => {
                                self.rows.insert(Self::key_to_string(&key), value);
                            }
                            ShapeMessage::Delete { key, .. } => {
                                self.rows.remove(&Self::key_to_string(&key));
                            }
                        }
                    }
                    self.offset = offset;
                }
            }
        }

        fn values(&self) -> Vec<Value> {
            self.rows.values().cloned().collect()
        }

        fn key_for_row(&self, row: &Value) -> String {
            let mut key = serde_json::Map::new();
            for column in &self.key_columns {
                key.insert(column.clone(), row[column].clone());
            }
            Self::key_to_string(&Value::Object(key))
        }

        fn key_to_string(key: &Value) -> String {
            serde_json::to_string(key).unwrap()
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

    async fn error_response(app: Router, uri: &str) -> (StatusCode, ErrorBody) {
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
        let parsed = serde_json::from_slice::<ErrorBody>(&body).unwrap();
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
                key_columns: vec!["id".to_string()],
                rows: vec![json!({"id": 1, "name": "Ada", "active": 1})],
                offset: 2,
                up_to_date: true,
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
                up_to_date: true,
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
    async fn denied_shape_returns_404_before_opening_sqlite() {
        let mut registry = ShapeRegistry::new();
        registry.add(active_users_shape());
        let app = router(ServerState::new("/missing/app.db", registry, DenyAll));

        let (status, body) =
            error_response(app, "/electrolite/v1/shape/activeUsers?offset=-1").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            body,
            ErrorBody {
                error: "shape_not_found".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn internal_errors_use_stable_public_body() {
        let mut registry = ShapeRegistry::new();
        registry.add(active_users_shape());
        let app = router(ServerState::new(
            "/missing/private/app.db",
            registry,
            AllowAll,
        ));

        let (status, body) =
            error_response(app, "/electrolite/v1/shape/activeUsers?offset=-1").await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            body,
            ErrorBody {
                error: "internal_server_error".to_string(),
            }
        );
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
                up_to_date: true,
            }
        );
    }

    #[tokio::test]
    async fn embedded_write_helper_wakes_live_request() {
        let (_dir, _path, state, app) = setup();
        let live = tokio::spawn(async move {
            json_response(app, "/electrolite/v1/shape/activeUsers?offset=2&live=true").await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        state
            .write(|conn| {
                conn.execute("UPDATE users SET active=1 WHERE id=2", [])?;
                Ok(())
            })
            .await
            .unwrap();

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
                up_to_date: true,
            }
        );
    }

    #[tokio::test]
    async fn embedded_retention_compaction_returns_resync_required() {
        let (_dir, _path, state, app) = setup();
        state
            .write(|conn| {
                conn.execute("UPDATE users SET active=1 WHERE id=2", [])?;
                Ok(())
            })
            .await
            .unwrap();

        let stats = state.compact_log_to_last(1).await.unwrap();
        assert_eq!(stats.retained_offset, 2);
        assert_eq!(stats.deleted_rows, 2);

        let (status, body) =
            error_response(app, "/electrolite/v1/shape/activeUsers?offset=1").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            body,
            ErrorBody {
                error: "resync_required".to_string(),
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
    async fn replay_older_than_retention_returns_resync_required() {
        let (_dir, path, _state, app) = setup();
        let conn = Connection::open(path).unwrap();
        conn.execute("UPDATE users SET active=1 WHERE id=2", [])
            .unwrap();
        conn.execute("DELETE FROM _electrolite_log WHERE seq <= 2", [])
            .unwrap();

        let (status, body) =
            error_response(app, "/electrolite/v1/shape/activeUsers?offset=0").await;

        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            body,
            ErrorBody {
                error: "resync_required".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn coalesced_live_requests_share_result() {
        let (_dir, path, state, app) = setup();
        let live_a = {
            let app = app.clone();
            tokio::spawn(async move {
                json_response(app, "/electrolite/v1/shape/activeUsers?offset=2&live=true").await
            })
        };
        let live_b = tokio::spawn(async move {
            json_response(app, "/electrolite/v1/shape/activeUsers?offset=2&live=true").await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let conn = Connection::open(path).unwrap();
        conn.execute("UPDATE users SET active=1 WHERE id=2", [])
            .unwrap();
        state.notify_changed();

        let result_a = live_a.await.unwrap();
        let result_b = live_b.await.unwrap();
        assert_eq!(result_a, result_b);
        assert_eq!(result_a.0, StatusCode::OK);
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
                key_columns: vec!["id".to_string()],
                rows: vec![json!({"id": 1, "name": "Ada", "active": 1})],
                offset: 2,
                up_to_date: true,
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
                up_to_date: true,
            }
        );
        server.abort();
    }

    #[tokio::test]
    async fn e2e_user_flow_materializes_authorized_live_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE todos (
              id INTEGER PRIMARY KEY,
              project_id TEXT NOT NULL,
              title TEXT NOT NULL,
              done INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO todos (id, project_id, title, done) VALUES
              (1, 'p1', 'ship electrolite', 0),
              (2, 'p2', 'other project', 0),
              (3, 'p1', 'hidden done item', 1);
            ",
        )
        .unwrap();
        electrolite_sqlite::install_triggers(&conn, "todos", "id").unwrap();

        let shape = Shape {
            name: "openProjectTodos".to_string(),
            table: "todos".to_string(),
            columns: vec![
                "id".to_string(),
                "project_id".to_string(),
                "title".to_string(),
                "done".to_string(),
            ],
            predicate: Predicate::And {
                predicates: vec![
                    Predicate::Eq {
                        column: "project_id".to_string(),
                        value: json!("p1"),
                    },
                    Predicate::Eq {
                        column: "done".to_string(),
                        value: json!(0),
                    },
                ],
            },
            auth_scope: "project:p1".to_string(),
            schema_version: 1,
        };
        let mut registry = ShapeRegistry::new();
        registry.add(shape);
        let state = ServerState::new(path.clone(), registry, HeaderScopeAuthorizer)
            .with_live_timeout(Duration::from_secs(2))
            .with_poll_interval(Duration::from_millis(10));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let base = format!("http://{addr}/electrolite/v1/shape/openProjectTodos");

        let denied = client
            .get(&base)
            .query(&[("offset", "-1")])
            .header("x-electrolite-scope", "project:p2")
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::NOT_FOUND);

        let mut materialized = MaterializedShape::new(&["id"]);
        let snapshot = client
            .get(&base)
            .query(&[("offset", "-1")])
            .header("x-electrolite-scope", "project:p1")
            .send()
            .await
            .unwrap()
            .json::<ShapeResponse>()
            .await
            .unwrap();
        materialized.apply(snapshot);
        assert_eq!(
            materialized.values(),
            vec![json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0})]
        );

        async fn live_after_write(
            client: &reqwest::Client,
            base: &str,
            offset: i64,
            state: &ServerState,
            path: &std::path::Path,
            sql: &str,
        ) -> ShapeResponse {
            let live = {
                let client = client.clone();
                let base = base.to_string();
                tokio::spawn(async move {
                    client
                        .get(base)
                        .query(&[("offset", offset.to_string()), ("live", "true".to_string())])
                        .header("x-electrolite-scope", "project:p1")
                        .send()
                        .await
                        .unwrap()
                        .json::<ShapeResponse>()
                        .await
                        .unwrap()
                })
            };
            tokio::time::sleep(Duration::from_millis(25)).await;
            let conn = Connection::open(path).unwrap();
            conn.execute(sql, []).unwrap();
            state.notify_changed();
            live.await.unwrap()
        }

        let replay = live_after_write(
            &client,
            &base,
            materialized.offset,
            &state,
            &path,
            "UPDATE todos SET done=0 WHERE id=3",
        )
        .await;
        materialized.apply(replay);
        assert_eq!(
            materialized.values(),
            vec![
                json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0}),
                json!({"id": 3, "project_id": "p1", "title": "hidden done item", "done": 0}),
            ]
        );

        let replay = live_after_write(
            &client,
            &base,
            materialized.offset,
            &state,
            &path,
            "UPDATE todos SET title='visible now' WHERE id=3",
        )
        .await;
        materialized.apply(replay);
        assert_eq!(
            materialized.values(),
            vec![
                json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0}),
                json!({"id": 3, "project_id": "p1", "title": "visible now", "done": 0}),
            ]
        );

        let replay = live_after_write(
            &client,
            &base,
            materialized.offset,
            &state,
            &path,
            "UPDATE todos SET done=1 WHERE id=3",
        )
        .await;
        materialized.apply(replay);
        assert_eq!(
            materialized.values(),
            vec![json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0})]
        );

        let replay = live_after_write(
            &client,
            &base,
            materialized.offset,
            &state,
            &path,
            "DELETE FROM todos WHERE id=1",
        )
        .await;
        materialized.apply(replay);
        assert_eq!(materialized.values(), Vec::<Value>::new());

        let replay = live_after_write(
            &client,
            &base,
            materialized.offset,
            &state,
            &path,
            "INSERT INTO todos (id, project_id, title, done) VALUES (4, 'p1', 'new visible', 0)",
        )
        .await;
        materialized.apply(replay);
        assert_eq!(
            materialized.values(),
            vec![json!({"id": 4, "project_id": "p1", "title": "new visible", "done": 0})]
        );

        let replay = live_after_write(
            &client,
            &base,
            materialized.offset,
            &state,
            &path,
            "UPDATE todos SET id=40 WHERE id=4",
        )
        .await;
        materialized.apply(replay);
        assert_eq!(
            materialized.values(),
            vec![json!({"id": 40, "project_id": "p1", "title": "new visible", "done": 0})]
        );

        server.abort();
    }

    #[tokio::test]
    async fn dynamic_factory_materializes_per_project_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let conn = Connection::open(&path).unwrap();
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
        .unwrap();
        electrolite_sqlite::install_triggers(&conn, "todos", "id").unwrap();
        conn.execute_batch(
            "
            INSERT INTO todos (id, project_id, title, done) VALUES
              (1, 'p1', 'ship electrolite', 0),
              (2, 'p2', 'other project', 0);
            ",
        )
        .unwrap();

        let state = ServerState::new(path.clone(), ShapeRegistry::new(), HeaderScopeAuthorizer)
            .with_shape_factory("projectTodos", ProjectTodosFactory)
            .with_live_timeout(Duration::from_secs(2))
            .with_poll_interval(Duration::from_millis(10));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let base = format!("http://{addr}/electrolite/v1/factory/projectTodos/p1");

        let denied = client
            .get(&base)
            .query(&[("offset", "-1")])
            .header("x-electrolite-project", "p2")
            .header("x-electrolite-scope", "project:p1")
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::NOT_FOUND);

        let mut materialized = MaterializedShape::new(&["id"]);
        let snapshot = client
            .get(&base)
            .query(&[("offset", "-1")])
            .header("x-electrolite-project", "p1")
            .header("x-electrolite-scope", "project:p1")
            .send()
            .await
            .unwrap()
            .json::<ShapeResponse>()
            .await
            .unwrap();
        materialized.apply(snapshot);
        assert_eq!(
            materialized.values(),
            vec![json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0})]
        );

        let ignored_project_write = {
            let client = client.clone();
            let base = base.clone();
            let offset = materialized.offset;
            tokio::spawn(async move {
                client
                    .get(base)
                    .query(&[("offset", offset.to_string()), ("live", "true".to_string())])
                    .header("x-electrolite-project", "p1")
                    .header("x-electrolite-scope", "project:p1")
                    .send()
                    .await
                    .unwrap()
                    .json::<ShapeResponse>()
                    .await
                    .unwrap()
            })
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        let conn = Connection::open(&path).unwrap();
        conn.execute("UPDATE todos SET title='not for p1' WHERE id=2", [])
            .unwrap();
        state.notify_changed();
        materialized.apply(ignored_project_write.await.unwrap());
        assert_eq!(
            materialized.values(),
            vec![json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0})]
        );

        let visible_project_write = {
            let client = client.clone();
            let base = base.clone();
            let offset = materialized.offset;
            tokio::spawn(async move {
                client
                    .get(base)
                    .query(&[("offset", offset.to_string()), ("live", "true".to_string())])
                    .header("x-electrolite-project", "p1")
                    .header("x-electrolite-scope", "project:p1")
                    .send()
                    .await
                    .unwrap()
                    .json::<ShapeResponse>()
                    .await
                    .unwrap()
            })
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO todos (id, project_id, title, done) VALUES (3, 'p1', 'visible', 0)",
            [],
        )
        .unwrap();
        state.notify_changed();
        materialized.apply(visible_project_write.await.unwrap());
        assert_eq!(
            materialized.values(),
            vec![
                json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0}),
                json!({"id": 3, "project_id": "p1", "title": "visible", "done": 0}),
            ]
        );

        let p1 = Shape {
            name: "projectTodos/p1".to_string(),
            table: "todos".to_string(),
            columns: vec![
                "id".to_string(),
                "project_id".to_string(),
                "title".to_string(),
                "done".to_string(),
            ],
            predicate: Predicate::Eq {
                column: "project_id".to_string(),
                value: json!("p1"),
            },
            auth_scope: "project:p1".to_string(),
            schema_version: 1,
        };
        let mut p2 = p1.clone();
        p2.name = "projectTodos/p2".to_string();
        p2.predicate = Predicate::Eq {
            column: "project_id".to_string(),
            value: json!("p2"),
        };
        p2.auth_scope = "project:p2".to_string();
        assert_ne!(p1.handle(), p2.handle());

        server.abort();
    }

    #[tokio::test]
    async fn trusted_header_factory_allows_typescript_defined_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let conn = Connection::open(&path).unwrap();
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
        .unwrap();
        electrolite_sqlite::install_triggers(&conn, "todos", "id").unwrap();
        conn.execute_batch(
            "
            INSERT INTO todos (id, project_id, title, done) VALUES
              (1, 'p1', 'ship electrolite', 0),
              (2, 'p2', 'other project', 0);
            ",
        )
        .unwrap();

        let state = ServerState::new(path, ShapeRegistry::new(), TrustedHeaderAuthorizer)
            .with_shape_factory(TRUSTED_SHAPE_FACTORY_NAME, TrustedHeaderShapeFactory);
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/electrolite/v1/factory/trusted/projectTodos/p1?offset=-1")
                    .header("x-electrolite-shape-name", "projectTodos/p1")
                    .header("x-electrolite-table", "todos")
                    .header(
                        "x-electrolite-columns",
                        r#"["id","project_id","title","done"]"#,
                    )
                    .header(
                        "x-electrolite-predicate",
                        r#"{"type":"eq","column":"project_id","value":"p1"}"#,
                    )
                    .header("x-electrolite-auth-scope", "project:p1")
                    .header("x-electrolite-schema-version", "1")
                    .header("x-electrolite-scope", "project:p1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<ShapeResponse>(&body).unwrap(),
            ShapeResponse::Snapshot {
                key_columns: vec!["id".to_string()],
                rows: vec![
                    json!({"id": 1, "project_id": "p1", "title": "ship electrolite", "done": 0})
                ],
                offset: 2,
                up_to_date: true,
            }
        );
    }

    #[tokio::test]
    async fn trusted_header_factory_rejects_malformed_shape_specs() {
        let app = router(
            ServerState::new("/missing/app.db", ShapeRegistry::new(), AllowAll)
                .with_shape_factory(TRUSTED_SHAPE_FACTORY_NAME, TrustedHeaderShapeFactory),
        );

        let (status, body) = error_response(
            app,
            "/electrolite/v1/factory/trusted/projectTodos/p1?offset=-1",
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            ErrorBody {
                error: "bad_shape_request".to_string(),
            }
        );
    }
}
