use electrolite_core::{LogOp, LogRow, Predicate, Replay, Shape, Snapshot};
use rusqlite::{Connection, OptionalExtension, ToSql, params};
use serde_json::{Number, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("table {table:?} has no primary-key column {pk_column:?}")]
    MissingPrimaryKey { table: String, pk_column: String },
    #[error("table {table:?} must have exactly one primary-key column, found {columns:?}")]
    UnsupportedPrimaryKey { table: String, columns: Vec<String> },
    #[error("table {table:?} has no columns")]
    EmptyTable { table: String },
    #[error("unknown electrolite log operation {0:?}")]
    InvalidLogOp(String),
    #[error("shape {shape:?} references table {table:?}, which is not watched")]
    ShapeTableMismatch { shape: String, table: String },
    #[error(
        "shape {shape:?} references table {table:?}, but Electrolite triggers are not installed"
    )]
    UnwatchedTable { shape: String, table: String },
    #[error("requested offset {requested_offset} is older than retained offset {retained_offset}")]
    ResyncRequired {
        requested_offset: i64,
        retained_offset: i64,
    },
    #[error("shape {shape:?} references missing column {column:?}")]
    MissingShapeColumn { shape: String, column: String },
    #[error("shape {shape:?} must include primary-key column {column:?}")]
    MissingShapePrimaryKey { shape: String, column: String },
    #[error("unsupported predicate in shape {shape:?}")]
    UnsupportedPredicate { shape: String },
    #[error("unsupported predicate value {value:?} in shape {shape:?}")]
    UnsupportedPredicateValue { shape: String, value: Value },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedTable {
    pub table: String,
    pub pk_column: String,
    pub columns: Vec<String>,
}

pub fn bootstrap(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS _electrolite_log (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          table_name TEXT NOT NULL,
          op TEXT NOT NULL,
          pk_json TEXT NOT NULL,
          old_pk_json TEXT,
          new_pk_json TEXT,
          old_json TEXT,
          new_json TEXT,
          created_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE INDEX IF NOT EXISTS _electrolite_log_table_seq
          ON _electrolite_log(table_name, seq);

        CREATE TABLE IF NOT EXISTS _electrolite_meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        ",
    )?;
    add_column_if_missing(conn, "_electrolite_log", "old_pk_json", "TEXT")?;
    add_column_if_missing(conn, "_electrolite_log", "new_pk_json", "TEXT")?;
    Ok(())
}

pub fn inspect_table(conn: &Connection, table: &str, pk_column: &str) -> Result<WatchedTable> {
    let (columns, primary_keys) = table_columns(conn, table)?;
    if primary_keys.len() != 1 {
        return Err(Error::UnsupportedPrimaryKey {
            table: table.to_string(),
            columns: primary_keys,
        });
    }
    let has_pk = primary_keys[0] == pk_column;
    if !has_pk {
        return Err(Error::MissingPrimaryKey {
            table: table.to_string(),
            pk_column: pk_column.to_string(),
        });
    }

    Ok(WatchedTable {
        table: table.to_string(),
        pk_column: pk_column.to_string(),
        columns,
    })
}

pub fn inspect_table_primary_key(conn: &Connection, table: &str) -> Result<WatchedTable> {
    let (columns, primary_keys) = table_columns(conn, table)?;
    let [pk_column] = primary_keys.as_slice() else {
        return Err(Error::UnsupportedPrimaryKey {
            table: table.to_string(),
            columns: primary_keys,
        });
    };

    Ok(WatchedTable {
        table: table.to_string(),
        pk_column: pk_column.clone(),
        columns,
    })
}

fn table_columns(conn: &Connection, table: &str) -> Result<(Vec<String>, Vec<String>)> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_string(table)))?;
    let mut rows = stmt.query([])?;
    let mut columns = Vec::new();
    let mut primary_keys = Vec::new();

    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let pk: i64 = row.get(5)?;
        if pk > 0 {
            primary_keys.push(name.clone());
        }
        columns.push(name);
    }

    if columns.is_empty() {
        return Err(Error::EmptyTable {
            table: table.to_string(),
        });
    }

    Ok((columns, primary_keys))
}

pub fn install_triggers(conn: &Connection, table: &str, pk_column: &str) -> Result<WatchedTable> {
    bootstrap(conn)?;
    let watched = inspect_table(conn, table, pk_column)?;
    install_triggers_for_watched(conn, watched)
}

pub fn install_triggers_auto(conn: &Connection, table: &str) -> Result<WatchedTable> {
    bootstrap(conn)?;
    let watched = inspect_table_primary_key(conn, table)?;
    install_triggers_for_watched(conn, watched)
}

fn install_triggers_for_watched(conn: &Connection, watched: WatchedTable) -> Result<WatchedTable> {
    let table_ident = quote_ident(&watched.table);
    let trigger_prefix = trigger_prefix(&watched.table);
    let pk_new = row_json_expr("NEW", &[watched.pk_column.clone()]);
    let pk_old = row_json_expr("OLD", &[watched.pk_column.clone()]);
    let new_row = row_json_expr("NEW", &watched.columns);
    let old_row = row_json_expr("OLD", &watched.columns);
    let table_lit = quote_string(&watched.table);

    conn.execute_batch(&format!(
        "
        DROP TRIGGER IF EXISTS {insert_trigger};
        DROP TRIGGER IF EXISTS {update_trigger};
        DROP TRIGGER IF EXISTS {delete_trigger};

        CREATE TRIGGER IF NOT EXISTS {insert_trigger}
        AFTER INSERT ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (
            table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
          )
          VALUES ({table_lit}, 'insert', {pk_new}, NULL, {pk_new}, NULL, {new_row});
        END;

        CREATE TRIGGER IF NOT EXISTS {update_trigger}
        AFTER UPDATE ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (
            table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
          )
          VALUES ({table_lit}, 'update', {pk_new}, {pk_old}, {pk_new}, {old_row}, {new_row});
        END;

        CREATE TRIGGER IF NOT EXISTS {delete_trigger}
        AFTER DELETE ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (
            table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
          )
          VALUES ({table_lit}, 'delete', {pk_old}, {pk_old}, NULL, {old_row}, NULL);
        END;
        ",
        insert_trigger = quote_ident(&format!("{trigger_prefix}_ai")),
        update_trigger = quote_ident(&format!("{trigger_prefix}_au")),
        delete_trigger = quote_ident(&format!("{trigger_prefix}_ad")),
    ))?;
    record_watched_table(conn, &watched)?;

    Ok(watched)
}

fn record_watched_table(conn: &Connection, watched: &WatchedTable) -> Result<()> {
    let value = serde_json::json!({
        "table": watched.table,
        "pk_column": watched.pk_column,
        "columns": watched.columns,
    })
    .to_string();
    conn.execute(
        "
        INSERT OR REPLACE INTO _electrolite_meta (key, value)
        VALUES (?1, ?2)
        ",
        params![watched_table_key(&watched.table), value],
    )?;
    Ok(())
}

pub fn read_log_since(
    conn: &Connection,
    table_name: &str,
    offset: i64,
    limit: i64,
) -> Result<Vec<LogRow>> {
    let mut stmt = conn.prepare(
        "
        SELECT seq, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json, created_at
        FROM _electrolite_log
        WHERE table_name = ?1 AND seq > ?2
        ORDER BY seq ASC
        LIMIT ?3
        ",
    )?;
    let rows = stmt.query_map(params![table_name, offset, limit], |row| {
        let pk_json: String = row.get(3)?;
        let old_pk_json: Option<String> = row.get(4)?;
        let new_pk_json: Option<String> = row.get(5)?;
        let old_json: Option<String> = row.get(6)?;
        let new_json: Option<String> = row.get(7)?;
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            pk_json,
            old_pk_json,
            new_pk_json,
            old_json,
            new_json,
            row.get::<_, i64>(8)?,
        ))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (
            seq,
            table_name,
            op_text,
            pk_json,
            old_pk_json,
            new_pk_json,
            old_json,
            new_json,
            created_at,
        ) = row?;
        let op = match op_text.as_str() {
            "insert" => LogOp::Insert,
            "update" => LogOp::Update,
            "delete" => LogOp::Delete,
            _ => return Err(Error::InvalidLogOp(op_text)),
        };
        out.push(LogRow {
            seq,
            table_name,
            op,
            pk_json: serde_json::from_str::<Value>(&pk_json)?,
            old_pk_json: old_pk_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            new_pk_json: new_pk_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            old_json: old_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            new_json: new_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            created_at,
        });
    }
    Ok(out)
}

pub fn initial_snapshot(conn: &Connection, shape: &Shape) -> Result<Snapshot> {
    bootstrap(conn)?;
    let watched = inspect_table_primary_key(conn, &shape.table)?;
    validate_shape_columns(&watched, shape)?;
    require_watched_table(conn, shape, &watched)?;

    let (where_sql, params) = compile_predicate(shape, &shape.predicate)?;
    let row_expr = row_json_expr("", &shape.columns);
    let mut sql = format!(
        "SELECT {row_expr} FROM {}",
        quote_ident(&shape.table),
        row_expr = row_expr
    );
    if !where_sql.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_sql);
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(&quote_ident(&watched.pk_column));

    let tx = conn.unchecked_transaction()?;
    let offset = tx.query_row(
        "SELECT COALESCE(MAX(seq), 0) FROM _electrolite_log",
        [],
        |r| r.get(0),
    )?;
    let params_ref: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();
    let out = {
        let mut stmt = tx.prepare(&sql)?;
        let rows = stmt.query_map(params_ref.as_slice(), |row| row.get::<_, String>(0))?;

        let mut out = Vec::new();
        for row in rows {
            out.push(serde_json::from_str::<Value>(&row?)?);
        }
        out
    };
    tx.commit()?;
    Ok(Snapshot { rows: out, offset })
}

fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_string(table)))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(());
        }
    }
    conn.execute_batch(&format!(
        "ALTER TABLE {} ADD COLUMN {} {definition}",
        quote_ident(table),
        quote_ident(column)
    ))?;
    Ok(())
}

pub fn replay(conn: &Connection, shape: &Shape, offset: i64, limit: i64) -> Result<Replay> {
    bootstrap(conn)?;
    let watched = inspect_table_primary_key(conn, &shape.table)?;
    validate_shape_columns(&watched, shape)?;
    require_watched_table(conn, shape, &watched)?;
    let retained_offset = retained_lower_bound(conn)?;
    if offset < retained_offset {
        return Err(Error::ResyncRequired {
            requested_offset: offset,
            retained_offset,
        });
    }

    let rows = read_log_since(conn, &shape.table, offset, limit)?;
    let mut messages = Vec::new();
    let mut latest = offset;

    for row in rows {
        latest = latest.max(row.seq);
        messages.extend(electrolite_core::messages_for_log(shape, &row));
    }

    Ok(Replay {
        messages,
        offset: latest,
    })
}

pub fn retained_lower_bound(conn: &Connection) -> Result<i64> {
    bootstrap(conn)?;
    let min_seq = conn.query_row("SELECT MIN(seq) FROM _electrolite_log", [], |row| {
        row.get::<_, Option<i64>>(0)
    })?;
    Ok(min_seq.map(|seq| seq - 1).unwrap_or(0))
}

fn require_watched_table(conn: &Connection, shape: &Shape, watched: &WatchedTable) -> Result<()> {
    let value = conn
        .query_row(
            "SELECT value FROM _electrolite_meta WHERE key = ?1",
            params![watched_table_key(&watched.table)],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(value) = value else {
        return Err(Error::UnwatchedTable {
            shape: shape.name.clone(),
            table: watched.table.clone(),
        });
    };

    let value = serde_json::from_str::<Value>(&value)?;
    let installed_pk = value.get("pk_column").and_then(Value::as_str);
    if installed_pk != Some(watched.pk_column.as_str()) {
        return Err(Error::UnwatchedTable {
            shape: shape.name.clone(),
            table: watched.table.clone(),
        });
    }

    Ok(())
}

pub fn high_water_mark(conn: &Connection) -> Result<i64> {
    bootstrap(conn)?;
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(seq), 0) FROM _electrolite_log",
        [],
        |r| r.get(0),
    )?)
}

fn validate_shape_columns(watched: &WatchedTable, shape: &Shape) -> Result<()> {
    if watched.table != shape.table {
        return Err(Error::ShapeTableMismatch {
            shape: shape.name.clone(),
            table: shape.table.clone(),
        });
    }

    for column in &shape.columns {
        if !watched.columns.contains(column) {
            return Err(Error::MissingShapeColumn {
                shape: shape.name.clone(),
                column: column.clone(),
            });
        }
    }

    if !shape.columns.contains(&watched.pk_column) {
        return Err(Error::MissingShapePrimaryKey {
            shape: shape.name.clone(),
            column: watched.pk_column.clone(),
        });
    }

    validate_predicate_columns(watched, shape, &shape.predicate)
}

fn validate_predicate_columns(
    watched: &WatchedTable,
    shape: &Shape,
    predicate: &Predicate,
) -> Result<()> {
    match predicate {
        Predicate::All => Ok(()),
        Predicate::Eq { column, .. } => {
            if watched.columns.contains(column) {
                Ok(())
            } else {
                Err(Error::MissingShapeColumn {
                    shape: shape.name.clone(),
                    column: column.clone(),
                })
            }
        }
        Predicate::And { predicates } => {
            for predicate in predicates {
                validate_predicate_columns(watched, shape, predicate)?;
            }
            Ok(())
        }
    }
}

fn compile_predicate(shape: &Shape, predicate: &Predicate) -> Result<(String, Vec<SqlParam>)> {
    match predicate {
        Predicate::All => Ok((String::new(), Vec::new())),
        Predicate::Eq { column, value } => {
            if value.is_null() {
                return Ok((format!("{} IS NULL", quote_ident(column)), Vec::new()));
            }
            let param = SqlParam::try_from_value(shape, value)?;
            Ok((format!("{} = ?", quote_ident(column)), vec![param]))
        }
        Predicate::And { predicates } => {
            let mut sql = Vec::new();
            let mut params = Vec::new();
            for predicate in predicates {
                let (part, mut part_params) = compile_predicate(shape, predicate)?;
                if part.is_empty() {
                    continue;
                }
                sql.push(format!("({part})"));
                params.append(&mut part_params);
            }
            Ok((sql.join(" AND "), params))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SqlParam {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Bool(bool),
}

impl SqlParam {
    fn try_from_value(shape: &Shape, value: &Value) -> Result<Self> {
        Ok(match value {
            Value::Null => Self::Null,
            Value::Bool(v) => Self::Bool(*v),
            Value::Number(n) => number_to_sql_param(shape, n)?,
            Value::String(s) => Self::Text(s.clone()),
            Value::Array(_) | Value::Object(_) => {
                return Err(Error::UnsupportedPredicateValue {
                    shape: shape.name.clone(),
                    value: value.clone(),
                });
            }
        })
    }
}

impl ToSql for SqlParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            Self::Null => Ok(rusqlite::types::Null.into()),
            Self::Integer(v) => Ok((*v).into()),
            Self::Real(v) => Ok((*v).into()),
            Self::Text(v) => Ok(v.as_str().into()),
            Self::Bool(v) => Ok((*v as i64).into()),
        }
    }
}

fn number_to_sql_param(shape: &Shape, n: &Number) -> Result<SqlParam> {
    if let Some(v) = n.as_i64() {
        Ok(SqlParam::Integer(v))
    } else if let Some(v) = n.as_u64().and_then(|v| i64::try_from(v).ok()) {
        Ok(SqlParam::Integer(v))
    } else if let Some(v) = n.as_f64() {
        Ok(SqlParam::Real(v))
    } else {
        Err(Error::UnsupportedPredicateValue {
            shape: shape.name.clone(),
            value: Value::Number(n.clone()),
        })
    }
}

fn row_json_expr(prefix: &str, columns: &[String]) -> String {
    let parts = columns.iter().flat_map(|column| {
        let value = if prefix.is_empty() {
            quote_ident(column)
        } else {
            format!("{prefix}.{}", quote_ident(column))
        };
        [quote_string(column), value]
    });
    format!("json_object({})", parts.collect::<Vec<_>>().join(", "))
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn trigger_prefix(table: &str) -> String {
    let mut out = String::from("_electrolite__");
    for ch in table.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

fn watched_table_key(table: &str) -> String {
    format!("watched_table:{table}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use electrolite_core::{Predicate, Shape, ShapeMessage};
    use serde_json::json;

    fn setup(conn: &Connection) {
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
        install_triggers(conn, "users", "id").unwrap();
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

    #[test]
    fn triggers_log_insert_update_delete() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
            [],
        )
        .unwrap();
        conn.execute("UPDATE users SET name='Ada Lovelace' WHERE id=1", [])
            .unwrap();
        conn.execute("DELETE FROM users WHERE id=1", []).unwrap();

        let rows = read_log_since(&conn, "users", 0, 10).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].op, LogOp::Insert);
        assert_eq!(rows[0].pk_json, json!({"id": 1}));
        assert_eq!(
            rows[0].new_json,
            Some(json!({"id": 1, "name": "Ada", "active": 1}))
        );

        assert_eq!(rows[1].op, LogOp::Update);
        assert_eq!(
            rows[1].old_json,
            Some(json!({"id": 1, "name": "Ada", "active": 1}))
        );
        assert_eq!(
            rows[1].new_json,
            Some(json!({"id": 1, "name": "Ada Lovelace", "active": 1}))
        );

        assert_eq!(rows[2].op, LogOp::Delete);
        assert_eq!(
            rows[2].old_json,
            Some(json!({"id": 1, "name": "Ada Lovelace", "active": 1}))
        );
        assert_eq!(rows[2].new_json, None);
    }

    #[test]
    fn rollback_writes_no_log_rows() {
        let mut conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let tx = conn.transaction().unwrap();
        tx.execute(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
            [],
        )
        .unwrap();
        tx.rollback().unwrap();

        let rows = read_log_since(&conn, "users", 0, 10).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn triggers_work_across_connections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        let conn1 = Connection::open(&path).unwrap();
        setup(&conn1);

        let conn2 = Connection::open(&path).unwrap();
        conn2
            .execute(
                "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
                [],
            )
            .unwrap();

        let rows = read_log_since(&conn1, "users", 0, 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].new_json,
            Some(json!({"id": 1, "name": "Ada", "active": 1}))
        );
    }

    #[test]
    fn snapshot_and_replay_shape_lifecycle() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = active_users_shape();

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

        let snapshot = initial_snapshot(&conn, &shape).unwrap();
        assert_eq!(snapshot.offset, 2);
        assert_eq!(
            snapshot.rows,
            vec![json!({"id": 1, "name": "Ada", "active": 1})]
        );

        conn.execute("UPDATE users SET active=1 WHERE id=2", [])
            .unwrap();
        let replayed = replay(&conn, &shape, snapshot.offset, 10).unwrap();
        assert_eq!(replayed.offset, 3);
        assert_eq!(replayed.messages.len(), 1);
        assert!(matches!(
            replayed.messages[0],
            ShapeMessage::Insert { ref key, ref value, offset: 3 }
                if *key == json!({"id": 2})
                    && *value == json!({"id": 2, "name": "Grace", "active": 1})
        ));

        conn.execute("UPDATE users SET name='Grace Hopper' WHERE id=2", [])
            .unwrap();
        let replayed = replay(&conn, &shape, replayed.offset, 10).unwrap();
        assert_eq!(replayed.offset, 4);
        assert_eq!(replayed.messages.len(), 1);
        assert!(matches!(
            replayed.messages[0],
            ShapeMessage::Update { ref key, ref value, offset: 4 }
                if *key == json!({"id": 2})
                    && *value == json!({"id": 2, "name": "Grace Hopper", "active": 1})
        ));

        conn.execute("UPDATE users SET active=0 WHERE id=2", [])
            .unwrap();
        let replayed = replay(&conn, &shape, replayed.offset, 10).unwrap();
        assert_eq!(replayed.offset, 5);
        assert_eq!(replayed.messages.len(), 1);
        assert!(matches!(
            replayed.messages[0],
            ShapeMessage::Delete { ref key, offset: 5 } if *key == json!({"id": 2})
        ));
    }

    #[test]
    fn snapshot_requires_triggers_to_be_installed() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              active INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1);
            ",
        )
        .unwrap();
        let shape = active_users_shape();

        let err = initial_snapshot(&conn, &shape).unwrap_err();
        assert!(matches!(
            err,
            Error::UnwatchedTable { ref shape, ref table }
                if shape == "activeUsers" && table == "users"
        ));
    }

    #[test]
    fn replay_requires_triggers_to_be_installed() {
        let conn = Connection::open_in_memory().unwrap();
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
        let shape = active_users_shape();

        let err = replay(&conn, &shape, 0, 10).unwrap_err();
        assert!(matches!(
            err,
            Error::UnwatchedTable { ref shape, ref table }
                if shape == "activeUsers" && table == "users"
        ));
    }

    #[test]
    fn replay_requires_resync_when_offset_is_older_than_retained_log() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = active_users_shape();

        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (2, 'Grace', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (3, 'Katherine', 1)",
            [],
        )
        .unwrap();
        conn.execute("DELETE FROM _electrolite_log WHERE seq <= 2", [])
            .unwrap();

        let err = replay(&conn, &shape, 0, 10).unwrap_err();
        assert!(matches!(
            err,
            Error::ResyncRequired {
                requested_offset: 0,
                retained_offset: 2,
            }
        ));

        let replayed = replay(&conn, &shape, 2, 10).unwrap();
        assert_eq!(replayed.offset, 3);
        assert_eq!(replayed.messages.len(), 1);
    }

    #[test]
    fn primary_key_update_replays_delete_then_insert() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = active_users_shape();

        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
            [],
        )
        .unwrap();
        let snapshot = initial_snapshot(&conn, &shape).unwrap();

        conn.execute("UPDATE users SET id=10 WHERE id=1", [])
            .unwrap();
        let replayed = replay(&conn, &shape, snapshot.offset, 10).unwrap();

        assert_eq!(replayed.offset, 2);
        assert_eq!(
            replayed.messages,
            vec![
                ShapeMessage::Delete {
                    key: json!({"id": 1}),
                    offset: 2,
                },
                ShapeMessage::Insert {
                    key: json!({"id": 10}),
                    value: json!({"id": 10, "name": "Ada", "active": 1}),
                    offset: 2,
                },
            ]
        );
    }

    #[test]
    fn null_equality_matches_snapshot_and_replay() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE people (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL,
              nickname TEXT
            );
            ",
        )
        .unwrap();
        install_triggers(&conn, "people", "id").unwrap();
        let shape = Shape {
            name: "unnicknamedPeople".to_string(),
            table: "people".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "nickname".to_string()],
            predicate: Predicate::Eq {
                column: "nickname".to_string(),
                value: Value::Null,
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        };

        conn.execute(
            "INSERT INTO people (id, name, nickname) VALUES (1, 'Ada', NULL)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO people (id, name, nickname) VALUES (2, 'Grace', 'Amazing Grace')",
            [],
        )
        .unwrap();
        let snapshot = initial_snapshot(&conn, &shape).unwrap();
        assert_eq!(
            snapshot.rows,
            vec![json!({"id": 1, "name": "Ada", "nickname": null})]
        );

        conn.execute("UPDATE people SET nickname=NULL WHERE id=2", [])
            .unwrap();
        let replayed = replay(&conn, &shape, snapshot.offset, 10).unwrap();
        assert_eq!(replayed.messages.len(), 1);
        assert!(matches!(
            replayed.messages[0],
            ShapeMessage::Insert { ref key, ref value, offset: 3 }
                if *key == json!({"id": 2})
                    && *value == json!({"id": 2, "name": "Grace", "nickname": null})
        ));
    }

    #[test]
    fn snapshot_discovers_non_id_primary_key() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE projects (
              slug TEXT PRIMARY KEY,
              title TEXT NOT NULL,
              public INTEGER NOT NULL DEFAULT 0
            );
            ",
        )
        .unwrap();
        install_triggers_auto(&conn, "projects").unwrap();
        let shape = Shape {
            name: "publicProjects".to_string(),
            table: "projects".to_string(),
            columns: vec![
                "slug".to_string(),
                "title".to_string(),
                "public".to_string(),
            ],
            predicate: Predicate::Eq {
                column: "public".to_string(),
                value: json!(1),
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        };

        conn.execute(
            "INSERT INTO projects (slug, title, public) VALUES ('electrolite', 'Electrolite', 1)",
            [],
        )
        .unwrap();
        let snapshot = initial_snapshot(&conn, &shape).unwrap();

        assert_eq!(
            snapshot.rows,
            vec![json!({"slug": "electrolite", "title": "Electrolite", "public": 1})]
        );
        let replayed = replay(&conn, &shape, 0, 10).unwrap();
        assert_eq!(
            replayed.messages,
            vec![ShapeMessage::Insert {
                key: json!({"slug": "electrolite"}),
                value: json!({"slug": "electrolite", "title": "Electrolite", "public": 1}),
                offset: 1,
            }]
        );
    }

    #[test]
    fn shape_columns_must_include_primary_key() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = Shape {
            name: "activeUserNames".to_string(),
            table: "users".to_string(),
            columns: vec!["name".to_string(), "active".to_string()],
            predicate: Predicate::Eq {
                column: "active".to_string(),
                value: json!(1),
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        };

        let err = initial_snapshot(&conn, &shape).unwrap_err();
        assert!(matches!(
            err,
            Error::MissingShapePrimaryKey { ref shape, ref column }
                if shape == "activeUserNames" && column == "id"
        ));
    }

    #[test]
    fn composite_primary_keys_are_explicitly_unsupported() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE memberships (
              account_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              role TEXT NOT NULL,
              PRIMARY KEY (account_id, user_id)
            );
            ",
        )
        .unwrap();

        let err = install_triggers(&conn, "memberships", "account_id").unwrap_err();
        assert!(matches!(
            err,
            Error::UnsupportedPrimaryKey { ref table, ref columns }
                if table == "memberships"
                    && columns == &vec!["account_id".to_string(), "user_id".to_string()]
        ));
    }
}
