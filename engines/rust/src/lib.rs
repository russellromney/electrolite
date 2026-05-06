//! Electrolite engine for Rust. Implements the conformance contract in
//! `engines/PROTOCOL.md`: install SQLite triggers, snapshot a shape,
//! replay logical changes, share a `batch_id` across `write_batch`,
//! detect log-id and retained-offset mismatches as `409
//! resync_required`, and serve named shapes through `handle()`.

use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension};
use serde_json::{json, Map, Value as Json};
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// ---------- predicates ----------

#[derive(Clone, Debug)]
pub enum RangeOp {
    Gt,
    Lt,
    Gte,
    Lte,
}

impl RangeOp {
    fn sql(&self) -> &'static str {
        match self {
            RangeOp::Gt => ">",
            RangeOp::Lt => "<",
            RangeOp::Gte => ">=",
            RangeOp::Lte => "<=",
        }
    }
    fn key(&self) -> &'static str {
        match self {
            RangeOp::Gt => "gt",
            RangeOp::Lt => "lt",
            RangeOp::Gte => "gte",
            RangeOp::Lte => "lte",
        }
    }
    pub fn from_key(k: &str) -> Option<Self> {
        match k {
            "gt" => Some(RangeOp::Gt),
            "lt" => Some(RangeOp::Lt),
            "gte" => Some(RangeOp::Gte),
            "lte" => Some(RangeOp::Lte),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Predicate {
    All,
    Eq { column: String, value: Json },
    Range { op: RangeOp, column: String, value: Json },
    In { column: String, values: Vec<Json> },
    And(Vec<Predicate>),
}

pub fn all() -> Predicate {
    Predicate::All
}
pub fn eq<S: Into<String>>(column: S, value: Json) -> Predicate {
    Predicate::Eq { column: column.into(), value }
}
pub fn gt<S: Into<String>>(column: S, value: Json) -> Predicate {
    Predicate::Range { op: RangeOp::Gt, column: column.into(), value }
}
pub fn lt<S: Into<String>>(column: S, value: Json) -> Predicate {
    Predicate::Range { op: RangeOp::Lt, column: column.into(), value }
}
pub fn gte<S: Into<String>>(column: S, value: Json) -> Predicate {
    Predicate::Range { op: RangeOp::Gte, column: column.into(), value }
}
pub fn lte<S: Into<String>>(column: S, value: Json) -> Predicate {
    Predicate::Range { op: RangeOp::Lte, column: column.into(), value }
}
pub fn in_list<S: Into<String>>(column: S, values: Vec<Json>) -> Predicate {
    Predicate::In { column: column.into(), values }
}
pub fn and(children: Vec<Predicate>) -> Predicate {
    Predicate::And(children)
}

// ---------- shapes ----------

#[derive(Clone, Debug)]
pub struct Shape {
    pub table: String,
    pub columns: Vec<String>,
    pub predicate: Predicate,
    pub auth_scope: String,
    pub schema_version: u32,
}

impl Shape {
    pub fn new<S: Into<String>>(table: S, columns: Vec<String>, predicate: Predicate) -> Self {
        Self {
            table: table.into(),
            columns,
            predicate,
            auth_scope: String::new(),
            schema_version: 1,
        }
    }
}

pub struct BuildContext<'a> {
    pub params: &'a HashMap<String, String>,
    pub context: &'a Json,
}

pub struct AuthContext<'a> {
    pub params: &'a HashMap<String, String>,
    pub context: &'a Json,
    pub scope: &'a str,
}

type WhereFn = Box<dyn Fn(&BuildContext) -> Predicate + Send + Sync>;
type ScopeFn = Box<dyn Fn(&BuildContext) -> String + Send + Sync>;
type AuthFn = Box<dyn Fn(&AuthContext) -> bool + Send + Sync>;

pub struct ShapeDef {
    pub table: String,
    pub columns: Vec<String>,
    pub params: Vec<String>,
    pub where_fn: Option<WhereFn>,
    pub scope_fn: Option<ScopeFn>,
    pub authorize_fn: Option<AuthFn>,
    pub schema_version: u32,
}

impl ShapeDef {
    pub fn new<S: Into<String>>(table: S, columns: Vec<String>) -> Self {
        Self {
            table: table.into(),
            columns,
            params: Vec::new(),
            where_fn: None,
            scope_fn: None,
            authorize_fn: None,
            schema_version: 1,
        }
    }
    pub fn params(mut self, params: Vec<String>) -> Self {
        self.params = params;
        self
    }
    pub fn where_fn(mut self, f: impl Fn(&BuildContext) -> Predicate + Send + Sync + 'static) -> Self {
        self.where_fn = Some(Box::new(f));
        self
    }
    pub fn scope_fn(mut self, f: impl Fn(&BuildContext) -> String + Send + Sync + 'static) -> Self {
        self.scope_fn = Some(Box::new(f));
        self
    }
    pub fn authorize_fn(mut self, f: impl Fn(&AuthContext) -> bool + Send + Sync + 'static) -> Self {
        self.authorize_fn = Some(Box::new(f));
        self
    }
}

// ---------- engine ----------

pub struct Electrolite {
    db: Mutex<Connection>,
    wake: Arc<(Mutex<u64>, Condvar)>,
    shapes: HashMap<String, ShapeDef>,
    prefix: String,
    replay_limit: i64,
    pub live_timeout: Duration,
}

#[derive(Debug)]
pub struct Snapshot {
    pub log_id: String,
    pub shape_handle: String,
    pub key_columns: Vec<String>,
    pub rows: Vec<Json>,
    pub offset: i64,
}

#[derive(Debug)]
pub struct Replay {
    pub log_id: String,
    pub shape_handle: String,
    pub messages: Vec<Json>,
    pub offset: i64,
    pub up_to_date: bool,
}

#[derive(Debug)]
pub struct CompactStats {
    pub retained_offset: i64,
    pub deleted_rows: usize,
}

#[derive(Debug)]
pub enum Error {
    Sqlite(rusqlite::Error),
    Bad(String),
    BadInput(String),
    ResyncRequired,
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Sqlite(e)
    }
}

impl Electrolite {
    pub fn open(path: &str) -> Result<Self, Error> {
        let db = Connection::open(path)?;
        let wake: Arc<(Mutex<u64>, Condvar)> = Arc::new((Mutex::new(0), Condvar::new()));

        // SQLite update_hook keeps the wake authoritative even when
        // writes bypass engine.execute() on this connection.
        let wake_for_hook = Arc::clone(&wake);
        db.update_hook(Some(
            move |_action, _db: &str, _tbl: &str, _rowid: i64| {
                let (lock, cv) = &*wake_for_hook;
                let mut g = lock.lock().unwrap();
                *g = g.wrapping_add(1);
                cv.notify_all();
            },
        ));

        let me = Self {
            db: Mutex::new(db),
            wake,
            shapes: HashMap::new(),
            prefix: "/electrolite/v1".to_string(),
            replay_limit: 1000,
            live_timeout: Duration::from_millis(20_000),
        };
        me.bootstrap()?;
        Ok(me)
    }

    pub fn add_shape<S: Into<String>>(&mut self, name: S, def: ShapeDef) -> &mut Self {
        self.shapes.insert(name.into(), def);
        self
    }

    pub fn set_prefix<S: Into<String>>(&mut self, prefix: S) -> &mut Self {
        self.prefix = prefix.into();
        self
    }

    pub fn set_replay_limit(&mut self, limit: i64) -> &mut Self {
        self.replay_limit = limit.max(1);
        self
    }

    fn bootstrap(&self) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        db.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS _electrolite_meta (
              key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS _electrolite_watched_tables (
              table_name TEXT PRIMARY KEY, pk_columns TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS _electrolite_log (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id TEXT NOT NULL,
              table_name TEXT NOT NULL,
              op TEXT NOT NULL,
              pk_json TEXT NOT NULL,
              old_pk_json TEXT, new_pk_json TEXT,
              old_json TEXT, new_json TEXT,
              created_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            CREATE INDEX IF NOT EXISTS _electrolite_log_table_seq_idx
              ON _electrolite_log (table_name, seq);
            "#,
        )?;
        let exists: Option<String> = db
            .query_row(
                "SELECT value FROM _electrolite_meta WHERE key = 'log_id'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if exists.is_none() {
            db.execute(
                "INSERT INTO _electrolite_meta (key, value) VALUES ('log_id', ?)",
                params![random_hex(16)],
            )?;
        }
        // A crashed write_batch may have left current_batch_id behind;
        // clear it so the next unrelated write does not inherit a dead
        // batch_id.
        db.execute(
            "DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'",
            [],
        )?;
        Ok(())
    }

    pub fn execute(&self, sql: &str, args: &[Value]) -> Result<usize, Error> {
        let db = self.db.lock().unwrap();
        Ok(db.execute(sql, params_from_iter(args.iter()))?)
    }

    pub fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let db = self.db.lock().unwrap();
        db.execute_batch(sql)?;
        Ok(())
    }

    pub fn install_triggers(&self, table: &str) -> Result<(), Error> {
        let info = self.inspect_table(table)?;
        if info.pk.is_empty() {
            return Err(Error::Bad(format!("table {table} must have a primary key")));
        }
        let db = self.db.lock().unwrap();
        db.execute(
            "INSERT INTO _electrolite_watched_tables (table_name, pk_columns) VALUES (?, ?) \
             ON CONFLICT(table_name) DO UPDATE SET pk_columns = excluded.pk_columns",
            params![table, serde_json::to_string(&info.pk).unwrap()],
        )?;

        let new_row = row_json("NEW", &info.columns);
        let old_row = row_json("OLD", &info.columns);
        let new_pk = row_json("NEW", &info.pk);
        let old_pk = row_json("OLD", &info.pk);
        let batch_id = "COALESCE((SELECT value FROM _electrolite_meta \
                        WHERE key = 'current_batch_id'), lower(hex(randomblob(16))))";
        let lit = quote_string(table);
        let tbl = quote_ident(table);
        db.execute_batch(&format!(
            r#"
            DROP TRIGGER IF EXISTS "_electrolite_{table}_ai";
            DROP TRIGGER IF EXISTS "_electrolite_{table}_au";
            DROP TRIGGER IF EXISTS "_electrolite_{table}_ad";
            CREATE TRIGGER "_electrolite_{table}_ai" AFTER INSERT ON {tbl} BEGIN
              INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
              VALUES ({batch_id}, {lit}, 'insert', {new_pk}, NULL, {new_pk}, NULL, {new_row});
            END;
            CREATE TRIGGER "_electrolite_{table}_au" AFTER UPDATE ON {tbl} BEGIN
              INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
              VALUES ({batch_id}, {lit}, 'update', {new_pk}, {old_pk}, {new_pk}, {old_row}, {new_row});
            END;
            CREATE TRIGGER "_electrolite_{table}_ad" AFTER DELETE ON {tbl} BEGIN
              INSERT INTO _electrolite_log (batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json)
              VALUES ({batch_id}, {lit}, 'delete', {old_pk}, {old_pk}, NULL, {old_row}, NULL);
            END;
            "#,
        ))?;
        Ok(())
    }

    pub fn write_batch(&self, statements: &[(&str, Vec<Value>)]) -> Result<(), Error> {
        let batch_id = random_hex(16);
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        tx.execute(
            "INSERT INTO _electrolite_meta (key, value) VALUES ('current_batch_id', ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![batch_id],
        )?;
        for (sql, args) in statements {
            tx.execute(sql, params_from_iter(args.iter()))?;
        }
        tx.execute(
            "DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn compact(&self, table: &str, keep_last: usize) -> Result<CompactStats, Error> {
        let db = self.db.lock().unwrap();
        let watermark: Option<i64> = db
            .query_row(
                "SELECT seq FROM _electrolite_log WHERE table_name = ? ORDER BY seq DESC LIMIT 1 OFFSET ?",
                params![table, keep_last as i64],
                |r| r.get(0),
            )
            .optional()?;
        let retained_offset = match watermark {
            Some(s) => s,
            None => high_water(&db)?,
        };
        let deleted = db.execute(
            "DELETE FROM _electrolite_log WHERE table_name = ? AND seq <= ?",
            params![table, retained_offset],
        )?;
        db.execute(
            "INSERT INTO _electrolite_meta (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![format!("retained_offset:{table}"), retained_offset.to_string()],
        )?;
        Ok(CompactStats {
            retained_offset,
            deleted_rows: deleted,
        })
    }

    pub fn snapshot(&self, shape: &Shape) -> Result<Snapshot, Error> {
        let info = self.watched_info(&shape.table)?;
        let normalized_predicate = normalize_predicate(&info, &shape.predicate)?;
        let normalized_shape = Shape {
            table: shape.table.clone(),
            columns: shape.columns.clone(),
            predicate: normalized_predicate.clone(),
            auth_scope: shape.auth_scope.clone(),
            schema_version: shape.schema_version,
        };
        let normalized = normalize_shape(&normalized_shape);
        let (where_sql, args) = compile_predicate(&normalized_predicate);
        let mut sql = format!(
            "SELECT {} AS row_json FROM {}",
            row_json("", &shape.columns),
            quote_ident(&shape.table)
        );
        if !where_sql.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_sql);
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(
            &info
                .pk
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(","),
        );
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare(&sql)?;
        let rows: Vec<Json> = stmt
            .query_map(params_from_iter(args.iter()), |r| {
                let s: String = r.get(0)?;
                Ok(serde_json::from_str(&s).unwrap())
            })?
            .collect::<Result<_, _>>()?;
        let offset = high_water(&db)?;
        let log_id = log_id(&db)?;
        Ok(Snapshot {
            log_id,
            shape_handle: handle(&normalized),
            key_columns: info.pk,
            rows,
            offset,
        })
    }

    pub fn replay(&self, shape: &Shape, offset: i64, limit: i64) -> Result<Replay, Error> {
        let info = self.watched_info(&shape.table)?;
        let normalized_predicate = normalize_predicate(&info, &shape.predicate)?;
        let normalized_shape = Shape {
            table: shape.table.clone(),
            columns: shape.columns.clone(),
            predicate: normalized_predicate.clone(),
            auth_scope: shape.auth_scope.clone(),
            schema_version: shape.schema_version,
        };
        let normalized = normalize_shape(&normalized_shape);
        let db = self.db.lock().unwrap();
        let retained = retained_offset(&db, &shape.table)?;
        if offset < retained {
            return Err(Error::ResyncRequired);
        }
        let mut stmt = db.prepare(
            "SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json \
             FROM _electrolite_log WHERE table_name = ? AND seq > ? ORDER BY seq LIMIT ?",
        )?;
        let mut rows: Vec<LogRow> = stmt
            .query_map(params![&shape.table, offset, limit.max(1)], parse_log_row)?
            .collect::<Result<_, _>>()?;
        if let Some(last) = rows.last().cloned() {
            let mut more = db.prepare(
                "SELECT seq, batch_id, op, pk_json, old_pk_json, new_pk_json, old_json, new_json \
                 FROM _electrolite_log WHERE table_name = ? AND seq > ? AND batch_id = ? ORDER BY seq",
            )?;
            let extra: Vec<LogRow> = more
                .query_map(
                    params![&shape.table, last.seq, last.batch_id],
                    parse_log_row,
                )?
                .collect::<Result<_, _>>()?;
            rows.extend(extra);
        }
        let latest = rows.last().map(|r| r.seq).unwrap_or(offset);
        let newer: Option<i64> = db
            .query_row(
                "SELECT 1 FROM _electrolite_log WHERE table_name = ? AND seq > ? LIMIT 1",
                params![&shape.table, latest],
                |r| r.get(0),
            )
            .optional()?;

        let mut messages = Vec::new();
        for r in &rows {
            messages.extend(messages_for(&normalized_predicate, r));
        }
        Ok(Replay {
            log_id: log_id(&db)?,
            shape_handle: handle(&normalized),
            messages,
            offset: latest,
            up_to_date: newer.is_none(),
        })
    }

    /// Block until the log changes or live_timeout elapses.
    pub fn wait_for_change(&self) {
        let (lock, cv) = &*self.wake;
        let lock = lock.lock().unwrap();
        let _ = cv.wait_timeout(lock, self.live_timeout).unwrap();
    }

    fn wait_for_change_until(&self, deadline: Instant) {
        let now = Instant::now();
        if deadline <= now {
            return;
        }
        let (lock, cv) = &*self.wake;
        let lock = lock.lock().unwrap();
        let _ = cv.wait_timeout(lock, deadline - now).unwrap();
    }

    pub fn handle(&self, path: &str, query: &str, context: &Json) -> (u16, Json) {
        let route = match self.parse_route(path, query) {
            Some(r) => r,
            None => return (404, json!({"error": "shape_not_found"})),
        };
        let def = match self.shapes.get(&route.name) {
            Some(d) => d,
            None => return (404, json!({"error": "shape_not_found"})),
        };

        let params: HashMap<String, String> = def
            .params
            .iter()
            .cloned()
            .zip(route.params.iter().cloned())
            .collect();
        if params.len() != def.params.len() {
            return (404, json!({"error": "shape_not_found"}));
        }
        let build_ctx = BuildContext { params: &params, context };
        let scope = def
            .scope_fn
            .as_ref()
            .map(|f| f(&build_ctx))
            .unwrap_or_default();
        let auth_ctx = AuthContext { params: &params, context, scope: &scope };
        if let Some(authorize) = &def.authorize_fn {
            if !authorize(&auth_ctx) {
                return (404, json!({"error": "shape_not_found"}));
            }
        }
        let predicate = def
            .where_fn
            .as_ref()
            .map(|f| f(&build_ctx))
            .unwrap_or(Predicate::All);

        let shape = Shape {
            table: def.table.clone(),
            columns: def.columns.clone(),
            predicate,
            auth_scope: scope.clone(),
            schema_version: def.schema_version,
        };
        let normalized = normalize_shape(&shape);
        let current_handle = handle(&normalized);
        let current_log_id = match self.current_log_id() {
            Ok(s) => s,
            Err(_) => return (500, json!({"error": "internal"})),
        };

        if route.offset >= 0 {
            if let Some(client_log_id) = &route.log_id {
                if client_log_id != &current_log_id {
                    return (409, json!({"error": "resync_required"}));
                }
            }
            if let Some(client_handle) = &route.shape_handle {
                if client_handle != &current_handle {
                    return (409, json!({"error": "resync_required"}));
                }
            }
        }

        if route.offset < 0 {
            return match self.snapshot(&shape) {
                Ok(s) => (200, snapshot_to_json(&s)),
                Err(Error::ResyncRequired) => (409, json!({"error": "resync_required"})),
                Err(Error::BadInput(msg)) => (400, json!({"error": "bad_request", "detail": msg})),
                Err(_) => (500, json!({"error": "internal"})),
            };
        }

        let body = match self.replay(&shape, route.offset, self.replay_limit) {
            Ok(b) => b,
            Err(Error::ResyncRequired) => return (409, json!({"error": "resync_required"})),
            Err(Error::BadInput(msg)) => {
                return (400, json!({"error": "bad_request", "detail": msg}))
            }
            Err(_) => return (500, json!({"error": "internal"})),
        };
        if route.live && body.messages.is_empty() && body.up_to_date {
            let deadline = Instant::now() + self.live_timeout;
            self.wait_for_change_until(deadline);
            let body = match self.replay(&shape, route.offset, self.replay_limit) {
                Ok(b) => b,
                Err(Error::ResyncRequired) => return (409, json!({"error": "resync_required"})),
                Err(Error::BadInput(msg)) => {
                    return (400, json!({"error": "bad_request", "detail": msg}))
                }
                Err(_) => return (500, json!({"error": "internal"})),
            };
            return (200, replay_to_json(&body));
        }
        (200, replay_to_json(&body))
    }

    fn parse_route(&self, path: &str, query: &str) -> Option<Route> {
        let prefix = format!("{}/", self.prefix);
        if !path.starts_with(&prefix) {
            return None;
        }
        let rest = &path[prefix.len()..];
        let parts: Vec<String> = rest
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if parts.is_empty() {
            return None;
        }
        let qp = parse_query(query);
        let offset = qp
            .get("offset")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(-1);
        let live = qp.get("live").map(|s| s == "true").unwrap_or(false);
        Some(Route {
            name: parts[0].clone(),
            params: parts[1..].to_vec(),
            offset,
            live,
            log_id: qp.get("log_id").cloned(),
            shape_handle: qp.get("shape_handle").cloned(),
        })
    }

    fn current_log_id(&self) -> Result<String, Error> {
        let db = self.db.lock().unwrap();
        log_id(&db)
    }

    fn inspect_table(&self, table: &str) -> Result<TableInfo, Error> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare(&format!("PRAGMA table_info({})", quote_string(table)))?;
        let mut cols: Vec<(i64, String, String, i64)> = stmt
            .query_map([], |r| {
                let cid: i64 = r.get(0)?;
                let name: String = r.get(1)?;
                let col_type: String = r.get(2).unwrap_or_default();
                let pk: i64 = r.get(5)?;
                Ok((cid, name, col_type, pk))
            })?
            .collect::<Result<_, _>>()?;
        if cols.is_empty() {
            return Err(Error::Bad(format!("table {table} does not exist")));
        }
        cols.sort_by_key(|c| c.0);
        let columns: Vec<String> = cols.iter().map(|c| c.1.clone()).collect();
        let column_types: HashMap<String, String> =
            cols.iter().map(|c| (c.1.clone(), c.2.clone())).collect();
        let mut pks: Vec<(i64, String)> = cols
            .into_iter()
            .filter(|c| c.3 > 0)
            .map(|c| (c.3, c.1))
            .collect();
        pks.sort_by_key(|c| c.0);
        let pk = pks.into_iter().map(|c| c.1).collect();
        Ok(TableInfo {
            columns,
            pk,
            column_types,
        })
    }

    fn watched_info(&self, table: &str) -> Result<TableInfo, Error> {
        let info = self.inspect_table(table)?;
        let db = self.db.lock().unwrap();
        let row: Option<String> = db
            .query_row(
                "SELECT pk_columns FROM _electrolite_watched_tables WHERE table_name = ?",
                params![table],
                |r| r.get(0),
            )
            .optional()?;
        let pk_json = row
            .ok_or_else(|| Error::Bad(format!("table {table} is not watched by Electrolite")))?;
        let pk: Vec<String> = serde_json::from_str(&pk_json).unwrap();
        Ok(TableInfo {
            columns: info.columns,
            pk,
            column_types: info.column_types,
        })
    }
}

struct Route {
    name: String,
    params: Vec<String>,
    offset: i64,
    live: bool,
    log_id: Option<String>,
    shape_handle: Option<String>,
}

struct TableInfo {
    columns: Vec<String>,
    pk: Vec<String>,
    column_types: HashMap<String, String>,
}

fn is_booleanish(decl: &str) -> bool {
    let upper = decl.to_uppercase();
    upper.contains("BOOL")
}

/// Coerce a single predicate value against a column's declared type.
/// Booleans against BOOLEAN-affinity columns become 0/1. Booleans
/// against any other column are an error. Null stays null.
fn normalize_value(info: &TableInfo, column: &str, value: &Json) -> Result<Json, Error> {
    if !info.columns.iter().any(|c| c == column) {
        return Err(Error::BadInput(format!(
            "predicate column {column} does not exist"
        )));
    }
    let column_type = info
        .column_types
        .get(column)
        .map(String::as_str)
        .unwrap_or("");
    match value {
        Json::Bool(b) => {
            if is_booleanish(column_type) {
                Ok(json!(if *b { 1 } else { 0 }))
            } else {
                Err(Error::BadInput(
                    "boolean predicates require BOOLEAN columns".into(),
                ))
            }
        }
        _ => Ok(value.clone()),
    }
}

/// Walk a predicate tree and coerce every value against the table info.
/// This is the single normalization site; everything downstream
/// (compile_predicate, predicate_matches, predicate_to_json) must
/// receive an already-normalized predicate.
fn normalize_predicate(info: &TableInfo, p: &Predicate) -> Result<Predicate, Error> {
    Ok(match p {
        Predicate::All => Predicate::All,
        Predicate::Eq { column, value } => Predicate::Eq {
            column: column.clone(),
            value: normalize_value(info, column, value)?,
        },
        Predicate::Range { op, column, value } => {
            if value.is_null() {
                return Err(Error::BadInput(format!(
                    "range predicate {} requires a non-null value",
                    op.key()
                )));
            }
            Predicate::Range {
                op: op.clone(),
                column: column.clone(),
                value: normalize_value(info, column, value)?,
            }
        }
        Predicate::In { column, values } => {
            let values = values
                .iter()
                .map(|v| normalize_value(info, column, v))
                .collect::<Result<Vec<_>, _>>()?;
            Predicate::In {
                column: column.clone(),
                values,
            }
        }
        Predicate::And(children) => Predicate::And(
            children
                .iter()
                .map(|c| normalize_predicate(info, c))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}

#[derive(Clone)]
struct LogRow {
    seq: i64,
    batch_id: String,
    #[allow(dead_code)]
    op: String,
    pk: Json,
    old_pk: Option<Json>,
    new_pk: Option<Json>,
    old_row: Option<Json>,
    new_row: Option<Json>,
}

fn parse_log_row(r: &rusqlite::Row) -> rusqlite::Result<LogRow> {
    let seq: i64 = r.get(0)?;
    let batch_id: String = r.get(1)?;
    let op: String = r.get(2)?;
    let pk: String = r.get(3)?;
    let old_pk: Option<String> = r.get(4)?;
    let new_pk: Option<String> = r.get(5)?;
    let old_row: Option<String> = r.get(6)?;
    let new_row: Option<String> = r.get(7)?;
    Ok(LogRow {
        seq,
        batch_id,
        op,
        pk: serde_json::from_str(&pk).unwrap(),
        old_pk: old_pk.map(|s| serde_json::from_str(&s).unwrap()),
        new_pk: new_pk.map(|s| serde_json::from_str(&s).unwrap()),
        old_row: old_row.map(|s| serde_json::from_str(&s).unwrap()),
        new_row: new_row.map(|s| serde_json::from_str(&s).unwrap()),
    })
}

fn predicate_matches(p: &Predicate, row: &Option<Json>) -> bool {
    let row = match row {
        Some(r) => r,
        None => return false,
    };
    match p {
        Predicate::All => true,
        Predicate::Eq { column, value } => row.get(column) == Some(value),
        Predicate::Range { op, column, value } => match row.get(column) {
            Some(left) => compare_json(left, value, op),
            None => false,
        },
        Predicate::In { column, values } => {
            let left = row.get(column);
            values.iter().any(|v| Some(v) == left)
        }
        Predicate::And(children) => children.iter().all(|c| predicate_matches(c, &Some(row.clone()))),
    }
}

fn compare_json(left: &Json, right: &Json, op: &RangeOp) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    let ord = match (left, right) {
        (Json::Number(a), Json::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) => x.partial_cmp(&y),
            _ => None,
        },
        (Json::String(a), Json::String(b)) => Some(a.cmp(b)),
        (Json::Bool(a), Json::Bool(b)) => Some(a.cmp(b)),
        _ => None,
    };
    match (ord, op) {
        (Some(o), RangeOp::Gt) => o == std::cmp::Ordering::Greater,
        (Some(o), RangeOp::Lt) => o == std::cmp::Ordering::Less,
        (Some(o), RangeOp::Gte) => o != std::cmp::Ordering::Less,
        (Some(o), RangeOp::Lte) => o != std::cmp::Ordering::Greater,
        _ => false,
    }
}

fn messages_for(p: &Predicate, r: &LogRow) -> Vec<Json> {
    let old_match = predicate_matches(p, &r.old_row);
    let new_match = predicate_matches(p, &r.new_row);
    let old_key = r.old_pk.clone().unwrap_or_else(|| r.pk.clone());
    let new_key = r.new_pk.clone().unwrap_or_else(|| r.pk.clone());
    if !old_match && new_match {
        if let Some(v) = &r.new_row {
            return vec![msg("insert", r, &new_key, Some(v))];
        }
    }
    if old_match && new_match {
        if let Some(v) = &r.new_row {
            if old_key == new_key {
                return vec![msg("update", r, &new_key, Some(v))];
            }
            return vec![msg("delete", r, &old_key, None), msg("insert", r, &new_key, Some(v))];
        }
    }
    if old_match && !new_match {
        return vec![msg("delete", r, &old_key, None)];
    }
    vec![]
}

fn msg(kind: &str, r: &LogRow, key: &Json, value: Option<&Json>) -> Json {
    let mut o = Map::new();
    o.insert("type".into(), json!(kind));
    o.insert("batch_id".into(), json!(r.batch_id));
    o.insert("key".into(), key.clone());
    o.insert("offset".into(), json!(r.seq));
    if let Some(v) = value {
        o.insert("value".into(), v.clone());
    }
    Json::Object(o)
}

fn compile_predicate(p: &Predicate) -> (String, Vec<Value>) {
    match p {
        Predicate::All => (String::new(), vec![]),
        Predicate::Eq { column, value } => {
            if value.is_null() {
                (format!("{} IS NULL", quote_ident(column)), vec![])
            } else {
                (format!("{} = ?", quote_ident(column)), vec![json_to_value(value)])
            }
        }
        Predicate::Range { op, column, value } => {
            if value.is_null() {
                ("0".to_string(), vec![])
            } else {
                (
                    format!("{} {} ?", quote_ident(column), op.sql()),
                    vec![json_to_value(value)],
                )
            }
        }
        Predicate::In { column, values } => {
            if values.is_empty() {
                return ("0".to_string(), vec![]);
            }
            let non_null: Vec<&Json> = values.iter().filter(|v| !v.is_null()).collect();
            let has_null = non_null.len() != values.len();
            let mut parts: Vec<String> = vec![];
            let mut args: Vec<Value> = vec![];
            if !non_null.is_empty() {
                let placeholders = vec!["?"; non_null.len()].join(",");
                parts.push(format!("{} IN ({})", quote_ident(column), placeholders));
                args.extend(non_null.iter().map(|v| json_to_value(v)));
            }
            if has_null {
                parts.push(format!("{} IS NULL", quote_ident(column)));
            }
            (
                parts
                    .into_iter()
                    .map(|p| format!("({})", p))
                    .collect::<Vec<_>>()
                    .join(" OR "),
                args,
            )
        }
        Predicate::And(children) => {
            let compiled: Vec<(String, Vec<Value>)> = children
                .iter()
                .map(compile_predicate)
                .filter(|(w, _)| !w.is_empty())
                .collect();
            let where_part = compiled
                .iter()
                .map(|(w, _)| format!("({})", w))
                .collect::<Vec<_>>()
                .join(" AND ");
            let args = compiled.into_iter().flat_map(|(_, a)| a).collect();
            (where_part, args)
        }
    }
}

fn json_to_value(v: &Json) -> Value {
    match v {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Real(n.as_f64().unwrap_or(0.0))
            }
        }
        Json::String(s) => Value::Text(s.clone()),
        _ => Value::Text(v.to_string()),
    }
}

fn predicate_to_json(p: &Predicate) -> Json {
    match p {
        Predicate::All => json!({"type": "all"}),
        Predicate::Eq { column, value } => json!({"type": "eq", "column": column, "value": value}),
        Predicate::Range { op, column, value } => {
            json!({"type": op.key(), "column": column, "value": value})
        }
        Predicate::In { column, values } => {
            let mut sorted: Vec<Json> = values.clone();
            sorted.sort_by(|a, b| {
                serde_json::to_string(a)
                    .unwrap()
                    .cmp(&serde_json::to_string(b).unwrap())
            });
            sorted.dedup_by(|a, b| serde_json::to_string(a).unwrap() == serde_json::to_string(b).unwrap());
            json!({"type": "in", "column": column, "values": sorted})
        }
        Predicate::And(children) => {
            let mut child_jsons: Vec<Json> = children.iter().map(predicate_to_json).collect();
            child_jsons.sort_by(|a, b| {
                serde_json::to_string(a)
                    .unwrap()
                    .cmp(&serde_json::to_string(b).unwrap())
            });
            json!({"type": "and", "predicates": child_jsons})
        }
    }
}

fn normalize_shape(shape: &Shape) -> Json {
    let mut cols = shape.columns.clone();
    cols.sort();
    json!({
        "auth_scope": shape.auth_scope,
        "columns": cols,
        "predicate": predicate_to_json(&shape.predicate),
        "schema_version": shape.schema_version,
        "table": shape.table,
    })
}

fn handle(normalized: &Json) -> String {
    use std::collections::BTreeMap;
    fn canonical(v: &Json) -> Json {
        match v {
            Json::Object(m) => {
                let sorted: BTreeMap<&String, Json> =
                    m.iter().map(|(k, v)| (k, canonical(v))).collect();
                let mut out = Map::new();
                for (k, v) in sorted {
                    out.insert(k.clone(), v);
                }
                Json::Object(out)
            }
            Json::Array(a) => Json::Array(a.iter().map(canonical).collect()),
            _ => v.clone(),
        }
    }
    let canon = canonical(normalized);
    let body = serde_json::to_string(&canon).unwrap();
    sha256_hex(body.as_bytes())
}

fn high_water(db: &Connection) -> Result<i64, Error> {
    Ok(db.query_row("SELECT COALESCE(MAX(seq), 0) FROM _electrolite_log", [], |r| r.get(0))?)
}

fn log_id(db: &Connection) -> Result<String, Error> {
    Ok(db.query_row(
        "SELECT value FROM _electrolite_meta WHERE key = 'log_id'",
        [],
        |r| r.get(0),
    )?)
}

fn retained_offset(db: &Connection, table: &str) -> Result<i64, Error> {
    let row: Option<String> = db
        .query_row(
            "SELECT value FROM _electrolite_meta WHERE key = ?",
            params![format!("retained_offset:{table}")],
            |r| r.get(0),
        )
        .optional()?;
    Ok(row.and_then(|s| s.parse().ok()).unwrap_or(0))
}

fn parse_query(query: &str) -> HashMap<String, String> {
    let q = query.trim_start_matches('?');
    let mut out = HashMap::new();
    if q.is_empty() {
        return out;
    }
    for pair in q.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        if !k.is_empty() {
            out.insert(k.to_string(), url_decode(v));
        }
    }
    out
}

fn url_decode(s: &str) -> String {
    // Minimal: handle %XX and +. Good enough for the tests we drive.
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16).unwrap_or(0);
                let lo = (bytes[i + 2] as char).to_digit(16).unwrap_or(0);
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_default()
}

fn snapshot_to_json(s: &Snapshot) -> Json {
    json!({
        "type": "snapshot",
        "log_id": s.log_id,
        "shape_handle": s.shape_handle,
        "key_columns": s.key_columns,
        "rows": s.rows,
        "offset": s.offset,
        "up_to_date": true,
    })
}

fn replay_to_json(r: &Replay) -> Json {
    json!({
        "type": "replay",
        "log_id": r.log_id,
        "shape_handle": r.shape_handle,
        "messages": r.messages,
        "offset": r.offset,
        "up_to_date": r.up_to_date,
    })
}

fn row_json(prefix: &str, columns: &[String]) -> String {
    let q = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}.")
    };
    let parts: Vec<String> = columns
        .iter()
        .map(|c| format!("{}, {}{}", quote_string(c), q, quote_ident(c)))
        .collect();
    format!("json_object({})", parts.join(", "))
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn random_hex(bytes: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut out = String::with_capacity(bytes * 2);
    for _ in 0..bytes {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        out.push_str(&format!("{:02x}", (seed & 0xff) as u8));
    }
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (bytes.len() as u64) * 8;
    let mut m = bytes.to_vec();
    m.push(0x80);
    while m.len() % 64 != 56 {
        m.push(0);
    }
    m.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in m.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let mj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(mj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    h.iter().map(|x| format!("{:08x}", x)).collect::<String>()
}
