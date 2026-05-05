use electrolite_core::{
    LogOp, LogRow, Predicate, Replay, Shape, ShapeCursor, ShapeReplayPage, Snapshot,
};
use rusqlite::{Connection, OptionalExtension, ToSql, params};
use serde_json::{Number, Value};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("table {table:?} has no primary-key column {pk_column:?}")]
    MissingPrimaryKey { table: String, pk_column: String },
    #[error("table {table:?} has no primary-key columns")]
    MissingPrimaryKeys { table: String },
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
    #[error(
        "unsupported shape column type {declared_type:?} for column {column:?} in shape {shape:?}"
    )]
    UnsupportedShapeColumnType {
        shape: String,
        column: String,
        declared_type: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchedTable {
    pub table: String,
    pub pk_columns: Vec<String>,
    pub columns: Vec<String>,
    column_types: HashMap<String, ColumnType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnInfo {
    name: String,
    declared_type: String,
    affinity: ColumnAffinity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnType {
    declared_type: String,
    affinity: ColumnAffinity,
}

impl ColumnType {
    fn is_booleanish(&self) -> bool {
        self.declared_type.to_ascii_uppercase().contains("BOOL")
    }

    fn is_blob(&self) -> bool {
        self.affinity == ColumnAffinity::Blob && !self.declared_type.trim().is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnAffinity {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

pub fn bootstrap(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS _electrolite_log (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id TEXT NOT NULL DEFAULT '',
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
    add_column_if_missing(
        conn,
        "_electrolite_log",
        "batch_id",
        "TEXT NOT NULL DEFAULT ''",
    )?;
    conn.execute(
        "UPDATE _electrolite_log SET batch_id = 'legacy-' || seq WHERE batch_id = ''",
        [],
    )?;
    add_column_if_missing(conn, "_electrolite_log", "old_pk_json", "TEXT")?;
    add_column_if_missing(conn, "_electrolite_log", "new_pk_json", "TEXT")?;
    Ok(())
}

pub fn inspect_table(conn: &Connection, table: &str, pk_column: &str) -> Result<WatchedTable> {
    let (column_infos, primary_keys) = table_columns(conn, table)?;
    if primary_keys != [pk_column.to_string()] {
        return Err(Error::MissingPrimaryKey {
            table: table.to_string(),
            pk_column: pk_column.to_string(),
        });
    }

    Ok(WatchedTable {
        table: table.to_string(),
        pk_columns: vec![pk_column.to_string()],
        columns: column_infos
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        column_types: column_types(column_infos),
    })
}

pub fn inspect_table_primary_key(conn: &Connection, table: &str) -> Result<WatchedTable> {
    let (column_infos, primary_keys) = table_columns(conn, table)?;
    if primary_keys.is_empty() {
        return Err(Error::MissingPrimaryKeys {
            table: table.to_string(),
        });
    }

    Ok(WatchedTable {
        table: table.to_string(),
        pk_columns: primary_keys,
        columns: column_infos
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        column_types: column_types(column_infos),
    })
}

fn table_columns(conn: &Connection, table: &str) -> Result<(Vec<ColumnInfo>, Vec<String>)> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_string(table)))?;
    let mut rows = stmt.query([])?;
    let mut columns = Vec::new();
    let mut primary_keys = Vec::new();

    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let declared_type: String = row.get(2)?;
        let pk: i64 = row.get(5)?;
        if pk > 0 {
            primary_keys.push((pk, name.clone()));
        }
        columns.push(ColumnInfo {
            name,
            affinity: column_affinity(&declared_type),
            declared_type,
        });
    }

    if columns.is_empty() {
        return Err(Error::EmptyTable {
            table: table.to_string(),
        });
    }

    primary_keys.sort_by_key(|(pk_order, _)| *pk_order);
    Ok((
        columns,
        primary_keys.into_iter().map(|(_, column)| column).collect(),
    ))
}

fn column_types(columns: Vec<ColumnInfo>) -> HashMap<String, ColumnType> {
    columns
        .into_iter()
        .map(|column| {
            (
                column.name,
                ColumnType {
                    declared_type: column.declared_type,
                    affinity: column.affinity,
                },
            )
        })
        .collect()
}

fn column_affinity(declared_type: &str) -> ColumnAffinity {
    let declared_type = declared_type.to_ascii_uppercase();
    if declared_type.contains("INT") {
        ColumnAffinity::Integer
    } else if declared_type.contains("CHAR")
        || declared_type.contains("CLOB")
        || declared_type.contains("TEXT")
    {
        ColumnAffinity::Text
    } else if declared_type.contains("BLOB") || declared_type.trim().is_empty() {
        ColumnAffinity::Blob
    } else if declared_type.contains("REAL")
        || declared_type.contains("FLOA")
        || declared_type.contains("DOUB")
    {
        ColumnAffinity::Real
    } else {
        ColumnAffinity::Numeric
    }
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

pub fn change_batch<T>(
    conn: &mut Connection,
    write: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T>,
) -> Result<T> {
    bootstrap(conn)?;
    let batch_id = format!(
        "{}-{}",
        unix_millis(),
        conn.query_row("SELECT lower(hex(randomblob(16)))", [], |row| {
            row.get::<_, String>(0)
        })?
    );
    let tx = conn.transaction()?;
    tx.execute(
        "
        INSERT OR REPLACE INTO _electrolite_meta (key, value)
        VALUES (?1, ?2)
        ",
        params![current_batch_id_key(), batch_id],
    )?;
    let out = write(&tx)?;
    tx.execute(
        "DELETE FROM _electrolite_meta WHERE key = ?1",
        params![current_batch_id_key()],
    )?;
    tx.commit()?;
    Ok(out)
}

fn install_triggers_for_watched(conn: &Connection, watched: WatchedTable) -> Result<WatchedTable> {
    let table_ident = quote_ident(&watched.table);
    let trigger_prefix = trigger_prefix(&watched.table);
    let pk_new = row_json_expr("NEW", &watched.pk_columns, &watched.column_types);
    let pk_old = row_json_expr("OLD", &watched.pk_columns, &watched.column_types);
    let new_row = row_json_expr("NEW", &watched.columns, &watched.column_types);
    let old_row = row_json_expr("OLD", &watched.columns, &watched.column_types);
    let table_lit = quote_string(&watched.table);
    let batch_id = batch_id_expr();

    conn.execute_batch(&format!(
        "
        DROP TRIGGER IF EXISTS {insert_trigger};
        DROP TRIGGER IF EXISTS {update_trigger};
        DROP TRIGGER IF EXISTS {delete_trigger};

        CREATE TRIGGER IF NOT EXISTS {insert_trigger}
        AFTER INSERT ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (
            batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
          )
          VALUES ({batch_id}, {table_lit}, 'insert', {pk_new}, NULL, {pk_new}, NULL, {new_row});
        END;

        CREATE TRIGGER IF NOT EXISTS {update_trigger}
        AFTER UPDATE ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (
            batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
          )
          VALUES ({batch_id}, {table_lit}, 'update', {pk_new}, {pk_old}, {pk_new}, {old_row}, {new_row});
        END;

        CREATE TRIGGER IF NOT EXISTS {delete_trigger}
        AFTER DELETE ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (
            batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
          )
          VALUES ({batch_id}, {table_lit}, 'delete', {pk_old}, {pk_old}, NULL, {old_row}, NULL);
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
        "pk_columns": watched.pk_columns,
        "columns": watched.columns,
        "column_types": watched.column_types.iter().map(|(column, column_type)| {
            (
                column.clone(),
                serde_json::json!({
                    "declared_type": column_type.declared_type,
                    "affinity": format!("{:?}", column_type.affinity),
                }),
            )
        }).collect::<serde_json::Map<_, _>>(),
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
    Ok(read_log_page_since(conn, table_name, offset, limit)?.rows)
}

#[derive(Debug)]
struct LogPage {
    rows: Vec<LogRow>,
    up_to_date: bool,
}

fn read_log_page_since(
    conn: &Connection,
    table_name: &str,
    offset: i64,
    limit: i64,
) -> Result<LogPage> {
    let limit = limit.max(0);
    let mut rows = read_log_since_limited(conn, table_name, offset, limit)?;
    if limit > 0 && rows.len() == limit as usize {
        if let Some(last) = rows.last() {
            let mut rest = read_log_batch_after(conn, table_name, last.seq, &last.batch_id)?;
            rows.append(&mut rest);
        }
    }

    let latest = rows.last().map(|row| row.seq).unwrap_or(offset);
    Ok(LogPage {
        rows,
        up_to_date: !table_has_log_after(conn, table_name, latest)?,
    })
}

fn table_has_log_after(conn: &Connection, table_name: &str, offset: i64) -> Result<bool> {
    Ok(conn.query_row(
        "
        SELECT EXISTS(
          SELECT 1 FROM _electrolite_log WHERE table_name = ?1 AND seq > ?2
        )
        ",
        params![table_name, offset],
        |row| row.get::<_, bool>(0),
    )?)
}

fn read_log_since_limited(
    conn: &Connection,
    table_name: &str,
    offset: i64,
    limit: i64,
) -> Result<Vec<LogRow>> {
    let mut stmt = conn.prepare(
        "
        SELECT seq, batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json, created_at
        FROM _electrolite_log
        WHERE table_name = ?1 AND seq > ?2
        ORDER BY seq ASC
        LIMIT ?3
        ",
    )?;
    log_rows_from_query(stmt.query_map(params![table_name, offset, limit], log_row_from_sql)?)
}

fn read_log_batch_after(
    conn: &Connection,
    table_name: &str,
    seq: i64,
    batch_id: &str,
) -> Result<Vec<LogRow>> {
    let mut stmt = conn.prepare(
        "
        SELECT seq, batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json, created_at
        FROM _electrolite_log
        WHERE table_name = ?1 AND seq > ?2 AND batch_id = ?3
        ORDER BY seq ASC
        ",
    )?;
    log_rows_from_query(stmt.query_map(params![table_name, seq, batch_id], log_row_from_sql)?)
}

fn log_rows_from_query(
    rows: impl Iterator<Item = rusqlite::Result<RawLogRow>>,
) -> Result<Vec<LogRow>> {
    let mut out = Vec::new();
    for row in rows {
        out.push(row?.try_into()?);
    }
    Ok(out)
}

#[derive(Debug)]
struct RawLogRow {
    seq: i64,
    batch_id: String,
    table_name: String,
    op_text: String,
    pk_json: String,
    old_pk_json: Option<String>,
    new_pk_json: Option<String>,
    old_json: Option<String>,
    new_json: Option<String>,
    created_at: i64,
}

fn log_row_from_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawLogRow> {
    Ok(RawLogRow {
        seq: row.get(0)?,
        batch_id: row.get(1)?,
        table_name: row.get(2)?,
        op_text: row.get(3)?,
        pk_json: row.get(4)?,
        old_pk_json: row.get(5)?,
        new_pk_json: row.get(6)?,
        old_json: row.get(7)?,
        new_json: row.get(8)?,
        created_at: row.get(9)?,
    })
}

impl TryFrom<RawLogRow> for LogRow {
    type Error = Error;

    fn try_from(row: RawLogRow) -> Result<Self> {
        let op = match row.op_text.as_str() {
            "insert" => LogOp::Insert,
            "update" => LogOp::Update,
            "delete" => LogOp::Delete,
            _ => return Err(Error::InvalidLogOp(row.op_text)),
        };
        Ok(Self {
            seq: row.seq,
            batch_id: row.batch_id,
            table_name: row.table_name,
            op,
            pk_json: serde_json::from_str::<Value>(&row.pk_json)?,
            old_pk_json: row
                .old_pk_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            new_pk_json: row
                .new_pk_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            old_json: row
                .old_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            new_json: row
                .new_json
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionStats {
    pub retained_offset: i64,
    pub deleted_rows: usize,
}

pub fn initial_snapshot(conn: &Connection, shape: &Shape) -> Result<Snapshot> {
    bootstrap(conn)?;
    let watched = inspect_table_primary_key(conn, &shape.table)?;
    validate_shape_columns(&watched, shape)?;
    require_watched_table(conn, shape, &watched)?;

    let (where_sql, params) = compile_predicate(&watched, shape, &shape.predicate)?;
    let row_expr = row_json_expr("", &shape.columns, &watched.column_types);
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
    sql.push_str(
        &watched
            .pk_columns
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", "),
    );

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
    Ok(Snapshot {
        key_columns: watched.pk_columns,
        rows: out,
        offset,
        up_to_date: true,
    })
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
    Ok(replay_page(conn, shape, offset, limit)?.replay())
}

pub fn replay_page(
    conn: &Connection,
    shape: &Shape,
    offset: i64,
    limit: i64,
) -> Result<ShapeReplayPage> {
    bootstrap(conn)?;
    let watched = inspect_table_primary_key(conn, &shape.table)?;
    validate_shape_columns(&watched, shape)?;
    require_watched_table(conn, shape, &watched)?;
    let normalized_shape = normalize_shape_predicate(&watched, shape)?;

    read_transaction(conn, |conn| {
        let retained_offset = retained_lower_bound_for_table_unbootstrapped(conn, &shape.table)?;
        if offset < retained_offset {
            return Err(Error::ResyncRequired {
                requested_offset: offset,
                retained_offset,
            });
        }

        let page = read_log_page_since(conn, &shape.table, offset, limit)?;
        let mut messages = Vec::new();
        let mut latest = offset;

        for row in page.rows {
            latest = latest.max(row.seq);
            messages.extend(electrolite_core::messages_for_log(&normalized_shape, &row));
        }

        Ok(ShapeReplayPage {
            cursor: ShapeCursor::new(&normalized_shape, latest, retained_offset),
            messages,
            source_offset_start: offset,
            source_offset_end: latest,
            up_to_date: page.up_to_date,
        })
    })
}

fn read_transaction<T>(
    conn: &Connection,
    read: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    conn.execute_batch("BEGIN DEFERRED TRANSACTION")?;
    let out = read(conn);
    match out {
        Ok(out) => {
            conn.execute_batch("COMMIT")?;
            Ok(out)
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn normalize_shape_predicate(watched: &WatchedTable, shape: &Shape) -> Result<Shape> {
    let mut normalized = shape.clone();
    normalized.predicate = normalize_predicate(watched, shape, &shape.predicate)?;
    Ok(normalized)
}

fn normalize_predicate(
    watched: &WatchedTable,
    shape: &Shape,
    predicate: &Predicate,
) -> Result<Predicate> {
    Ok(match predicate {
        Predicate::All => Predicate::All,
        Predicate::Eq { column, value } => Predicate::Eq {
            column: column.clone(),
            value: normalize_predicate_value(shape, column_type(watched, shape, column)?, value)?,
        },
        Predicate::In { column, values } => Predicate::In {
            column: column.clone(),
            values: values
                .iter()
                .map(|value| {
                    normalize_predicate_value(shape, column_type(watched, shape, column)?, value)
                })
                .collect::<Result<Vec<_>>>()?,
        },
        Predicate::And { predicates } => Predicate::And {
            predicates: predicates
                .iter()
                .map(|predicate| normalize_predicate(watched, shape, predicate))
                .collect::<Result<Vec<_>>>()?,
        },
    })
}

fn normalize_predicate_value(
    shape: &Shape,
    column_type: &ColumnType,
    value: &Value,
) -> Result<Value> {
    Ok(match SqlParam::try_from_value(shape, column_type, value)? {
        SqlParam::Null => Value::Null,
        SqlParam::Integer(value) => Value::Number(value.into()),
        SqlParam::Real(value) => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| Error::UnsupportedPredicateValue {
                shape: shape.name.clone(),
                value: value.into(),
            })?,
        SqlParam::Text(value) => Value::String(value),
    })
}

pub fn retained_lower_bound(conn: &Connection) -> Result<i64> {
    bootstrap(conn)?;
    retained_lower_bound_unbootstrapped(conn)
}

fn retained_lower_bound_unbootstrapped(conn: &Connection) -> Result<i64> {
    let stored_offset = conn
        .query_row(
            "SELECT value FROM _electrolite_meta WHERE key = ?1",
            params![retained_offset_key()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let min_seq = conn.query_row("SELECT MIN(seq) FROM _electrolite_log", [], |row| {
        row.get::<_, Option<i64>>(0)
    })?;
    Ok(stored_offset.max(min_seq.map(|seq| seq - 1).unwrap_or(0)))
}

pub fn retained_lower_bound_for_table(conn: &Connection, table_name: &str) -> Result<i64> {
    bootstrap(conn)?;
    retained_lower_bound_for_table_unbootstrapped(conn, table_name)
}

fn retained_lower_bound_for_table_unbootstrapped(
    conn: &Connection,
    table_name: &str,
) -> Result<i64> {
    let stored_offset = conn
        .query_row(
            "SELECT value FROM _electrolite_meta WHERE key = ?1",
            params![retained_offset_table_key(table_name)],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let min_seq = conn.query_row(
        "SELECT MIN(seq) FROM _electrolite_log WHERE table_name = ?1",
        params![table_name],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    Ok(stored_offset.max(min_seq.map(|seq| seq - 1).unwrap_or(0)))
}

pub fn compact_log_before(conn: &Connection, retained_offset: i64) -> Result<RetentionStats> {
    bootstrap(conn)?;
    let retained_offset = retained_offset.max(retained_lower_bound(conn)?);
    let table_offsets = retained_table_offsets_before(conn, retained_offset)?;
    let deleted_rows = conn.execute(
        "DELETE FROM _electrolite_log WHERE seq <= ?1",
        params![retained_offset],
    )?;
    record_retained_offset(conn, retained_offset)?;
    for (table_name, table_offset) in table_offsets {
        record_retained_offset_for_table(conn, &table_name, table_offset)?;
    }
    Ok(RetentionStats {
        retained_offset,
        deleted_rows,
    })
}

fn retained_table_offsets_before(
    conn: &Connection,
    retained_offset: i64,
) -> Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(
        "
        SELECT table_name, MAX(seq)
        FROM _electrolite_log
        WHERE seq <= ?1
        GROUP BY table_name
        ",
    )?;
    let rows = stmt.query_map(params![retained_offset], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn compact_log_before_for_table(
    conn: &Connection,
    table_name: &str,
    retained_offset: i64,
) -> Result<RetentionStats> {
    bootstrap(conn)?;
    let retained_offset = retained_offset.max(retained_lower_bound_for_table(conn, table_name)?);
    let deleted_rows = conn.execute(
        "DELETE FROM _electrolite_log WHERE table_name = ?1 AND seq <= ?2",
        params![table_name, retained_offset],
    )?;
    record_retained_offset_for_table(conn, table_name, retained_offset)?;
    Ok(RetentionStats {
        retained_offset,
        deleted_rows,
    })
}

pub fn compact_log_to_last_for_table(
    conn: &Connection,
    table_name: &str,
    keep_last: i64,
) -> Result<RetentionStats> {
    bootstrap(conn)?;
    let keep_last = keep_last.max(0);
    let high_water = conn.query_row(
        "SELECT COALESCE(MAX(seq), 0) FROM _electrolite_log WHERE table_name = ?1",
        params![table_name],
        |row| row.get::<_, i64>(0),
    )?;
    let retained_offset = high_water.saturating_sub(keep_last);
    compact_log_before_for_table(conn, table_name, retained_offset)
}

pub fn compact_log_to_last(conn: &Connection, keep_last: i64) -> Result<RetentionStats> {
    bootstrap(conn)?;
    let keep_last = keep_last.max(0);
    let high_water = high_water_mark(conn)?;
    let retained_offset = high_water.saturating_sub(keep_last);
    compact_log_before(conn, retained_offset)
}

fn record_retained_offset(conn: &Connection, retained_offset: i64) -> Result<()> {
    conn.execute(
        "
        INSERT OR REPLACE INTO _electrolite_meta (key, value)
        VALUES (?1, ?2)
        ",
        params![retained_offset_key(), retained_offset.to_string()],
    )?;
    Ok(())
}

fn record_retained_offset_for_table(
    conn: &Connection,
    table_name: &str,
    retained_offset: i64,
) -> Result<()> {
    conn.execute(
        "
        INSERT OR REPLACE INTO _electrolite_meta (key, value)
        VALUES (?1, ?2)
        ",
        params![
            retained_offset_table_key(table_name),
            retained_offset.to_string()
        ],
    )?;
    Ok(())
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
    let installed_pk_columns = value
        .get("pk_columns")
        .and_then(Value::as_array)
        .map(|columns| {
            columns
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .or_else(|| {
            value
                .get("pk_column")
                .and_then(Value::as_str)
                .map(|column| vec![column.to_string()])
        });
    if installed_pk_columns.as_deref() != Some(watched.pk_columns.as_slice()) {
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
        require_supported_shape_column(watched, shape, column)?;
    }

    for pk_column in &watched.pk_columns {
        if !shape.columns.contains(pk_column) {
            return Err(Error::MissingShapePrimaryKey {
                shape: shape.name.clone(),
                column: pk_column.clone(),
            });
        }
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
        Predicate::Eq { column, .. } | Predicate::In { column, .. } => {
            if watched.columns.contains(column) {
                require_supported_shape_column(watched, shape, column)?;
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

fn require_supported_shape_column(
    watched: &WatchedTable,
    shape: &Shape,
    column: &str,
) -> Result<()> {
    let Some(column_type) = watched.column_types.get(column) else {
        return Err(Error::MissingShapeColumn {
            shape: shape.name.clone(),
            column: column.to_string(),
        });
    };
    if column_type.is_blob() {
        Err(Error::UnsupportedShapeColumnType {
            shape: shape.name.clone(),
            column: column.to_string(),
            declared_type: column_type.declared_type.clone(),
        })
    } else {
        Ok(())
    }
}

fn compile_predicate(
    watched: &WatchedTable,
    shape: &Shape,
    predicate: &Predicate,
) -> Result<(String, Vec<SqlParam>)> {
    match predicate {
        Predicate::All => Ok((String::new(), Vec::new())),
        Predicate::Eq { column, value } => {
            if value.is_null() {
                return Ok((format!("{} IS NULL", quote_ident(column)), Vec::new()));
            }
            let column_type = column_type(watched, shape, column)?;
            let param = SqlParam::try_from_value(shape, column_type, value)?;
            Ok((format!("{} = ?", quote_ident(column)), vec![param]))
        }
        Predicate::In { column, values } => compile_in_predicate(watched, shape, column, values),
        Predicate::And { predicates } => {
            let mut sql = Vec::new();
            let mut params = Vec::new();
            for predicate in predicates {
                let (part, mut part_params) = compile_predicate(watched, shape, predicate)?;
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

fn compile_in_predicate(
    watched: &WatchedTable,
    shape: &Shape,
    column: &str,
    values: &[Value],
) -> Result<(String, Vec<SqlParam>)> {
    if values.is_empty() {
        return Ok(("0".to_string(), Vec::new()));
    }

    let mut has_null = false;
    let mut params = Vec::new();
    let column_type = column_type(watched, shape, column)?;
    for value in values {
        if value.is_null() {
            has_null = true;
        } else {
            params.push(SqlParam::try_from_value(shape, column_type, value)?);
        }
    }

    let column = quote_ident(column);
    let mut parts = Vec::new();
    if !params.is_empty() {
        let placeholders = std::iter::repeat_n("?", params.len())
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!("{column} IN ({placeholders})"));
    }
    if has_null {
        parts.push(format!("{column} IS NULL"));
    }

    Ok((parts.join(" OR "), params))
}

fn column_type<'a>(
    watched: &'a WatchedTable,
    shape: &Shape,
    column: &str,
) -> Result<&'a ColumnType> {
    watched
        .column_types
        .get(column)
        .ok_or_else(|| Error::MissingShapeColumn {
            shape: shape.name.clone(),
            column: column.to_string(),
        })
}

#[derive(Debug, Clone, PartialEq)]
enum SqlParam {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl SqlParam {
    fn try_from_value(shape: &Shape, column_type: &ColumnType, value: &Value) -> Result<Self> {
        Ok(match (column_type.affinity, value) {
            (_, Value::Null) => Self::Null,
            (_, Value::Bool(v)) if column_type.is_booleanish() => Self::Integer(i64::from(*v)),
            (ColumnAffinity::Integer, Value::Number(n))
            | (ColumnAffinity::Numeric, Value::Number(n))
                if n.as_i64().is_some()
                    || n.as_u64().and_then(|v| i64::try_from(v).ok()).is_some() =>
            {
                number_to_integer_sql_param(shape, n)?
            }
            (ColumnAffinity::Real, Value::Number(n))
            | (ColumnAffinity::Numeric, Value::Number(n)) => number_to_sql_param(shape, n)?,
            (ColumnAffinity::Text, Value::String(s)) => Self::Text(s.clone()),
            (ColumnAffinity::Blob, _) if column_type.declared_type.trim().is_empty() => {
                value_to_untyped_sql_param(shape, value)?
            }
            (_, Value::Array(_) | Value::Object(_)) => {
                return Err(Error::UnsupportedPredicateValue {
                    shape: shape.name.clone(),
                    value: value.clone(),
                });
            }
            _ => {
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

fn number_to_integer_sql_param(shape: &Shape, n: &Number) -> Result<SqlParam> {
    if let Some(v) = n.as_i64() {
        Ok(SqlParam::Integer(v))
    } else if let Some(v) = n.as_u64().and_then(|v| i64::try_from(v).ok()) {
        Ok(SqlParam::Integer(v))
    } else {
        Err(Error::UnsupportedPredicateValue {
            shape: shape.name.clone(),
            value: Value::Number(n.clone()),
        })
    }
}

fn value_to_untyped_sql_param(shape: &Shape, value: &Value) -> Result<SqlParam> {
    Ok(match value {
        Value::Null => SqlParam::Null,
        Value::Bool(_) => {
            return Err(Error::UnsupportedPredicateValue {
                shape: shape.name.clone(),
                value: value.clone(),
            });
        }
        Value::Number(n) => number_to_sql_param(shape, n)?,
        Value::String(s) => SqlParam::Text(s.clone()),
        Value::Array(_) | Value::Object(_) => {
            return Err(Error::UnsupportedPredicateValue {
                shape: shape.name.clone(),
                value: value.clone(),
            });
        }
    })
}

fn row_json_expr(
    prefix: &str,
    columns: &[String],
    column_types: &HashMap<String, ColumnType>,
) -> String {
    let parts = columns.iter().flat_map(|column| {
        let base = if prefix.is_empty() {
            quote_ident(column)
        } else {
            format!("{prefix}.{}", quote_ident(column))
        };
        let value = normalize_row_value_expr(&base, column_types.get(column));
        [quote_string(column), value]
    });
    format!("json_object({})", parts.collect::<Vec<_>>().join(", "))
}

fn normalize_row_value_expr(base: &str, column_type: Option<&ColumnType>) -> String {
    let Some(column_type) = column_type else {
        return base.to_string();
    };
    match column_type.affinity {
        ColumnAffinity::Integer => format!("CAST({base} AS INTEGER)"),
        ColumnAffinity::Real => format!("CAST({base} AS REAL)"),
        ColumnAffinity::Text => format!("CAST({base} AS TEXT)"),
        ColumnAffinity::Numeric if column_type.is_booleanish() => {
            format!("CAST({base} AS INTEGER)")
        }
        ColumnAffinity::Numeric | ColumnAffinity::Blob => base.to_string(),
    }
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

fn retained_offset_key() -> &'static str {
    "retained_offset"
}

fn retained_offset_table_key(table_name: &str) -> String {
    format!("retained_offset:{table_name}")
}

fn current_batch_id_key() -> &'static str {
    "current_batch_id"
}

fn batch_id_expr() -> String {
    format!(
        "COALESCE((SELECT value FROM _electrolite_meta WHERE key = {}), lower(hex(randomblob(16))))",
        quote_string(current_batch_id_key())
    )
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use electrolite_core::{Predicate, Shape, ShapeMessage};
    use serde_json::json;
    use std::collections::BTreeMap;

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

    #[derive(Debug)]
    struct MaterializedShape {
        key_columns: Vec<String>,
        offset: i64,
        rows: BTreeMap<String, Value>,
        pending_rows: Option<BTreeMap<String, Value>>,
        pending_changed: bool,
    }

    impl MaterializedShape {
        fn from_snapshot(snapshot: Snapshot) -> Self {
            let mut out = Self {
                key_columns: snapshot.key_columns,
                offset: snapshot.offset,
                rows: BTreeMap::new(),
                pending_rows: None,
                pending_changed: false,
            };
            for row in snapshot.rows {
                out.rows.insert(out.key_for_row(&row), row);
            }
            out
        }

        fn apply_replay(&mut self, replay: Replay) -> bool {
            let mut changed = false;
            let mut rows = self
                .pending_rows
                .clone()
                .unwrap_or_else(|| self.rows.clone());
            for message in replay.messages {
                changed = Self::apply_message_to(&mut rows, message) || changed;
            }
            self.offset = replay.offset;
            if !replay.up_to_date {
                self.pending_rows = Some(rows);
                self.pending_changed = self.pending_changed || changed;
                return false;
            }

            changed = self.pending_changed || changed;
            self.rows = rows;
            self.pending_rows = None;
            self.pending_changed = false;
            changed
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

        fn apply_message_to(rows: &mut BTreeMap<String, Value>, message: ShapeMessage) -> bool {
            match message {
                ShapeMessage::Insert { key, value, .. }
                | ShapeMessage::Update { key, value, .. } => {
                    rows.insert(Self::key_to_string(&key), value);
                    true
                }
                ShapeMessage::Delete { key, .. } => {
                    rows.remove(&Self::key_to_string(&key)).is_some()
                }
            }
        }

        fn key_to_string(key: &Value) -> String {
            serde_json::to_string(key).unwrap()
        }
    }

    fn authoritative_rows(conn: &Connection, shape: &Shape) -> Vec<Value> {
        let snapshot = initial_snapshot(conn, shape).unwrap();
        let materialized = MaterializedShape::from_snapshot(snapshot);
        materialized.values()
    }

    fn sync_until_up_to_date(
        conn: &Connection,
        shape: &Shape,
        materialized: &mut MaterializedShape,
        limit: i64,
    ) -> usize {
        let mut pages = 0;
        loop {
            pages += 1;
            let replayed = replay(conn, shape, materialized.offset, limit).unwrap();
            let up_to_date = replayed.up_to_date;
            materialized.apply_replay(replayed);
            if up_to_date {
                return pages;
            }
            assert!(pages < 32, "replay did not reach up_to_date");
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
    fn change_batch_marks_rows_and_replay_does_not_split_batch() {
        let mut conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = active_users_shape();

        change_batch(&mut conn, |tx| {
            tx.execute(
                "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO users (id, name, active) VALUES (2, 'Grace', 1)",
                [],
            )?;
            tx.execute(
                "INSERT INTO users (id, name, active) VALUES (3, 'Katherine', 1)",
                [],
            )?;
            Ok(())
        })
        .unwrap();

        let rows = read_log_since(&conn, "users", 0, 1).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|row| row.batch_id == rows[0].batch_id));

        let replayed = replay(&conn, &shape, 0, 1).unwrap();
        assert_eq!(replayed.offset, 3);
        assert_eq!(replayed.messages.len(), 3);
    }

    #[test]
    fn failed_change_batch_rolls_back_log_rows() {
        let mut conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        let err = change_batch(&mut conn, |tx| {
            tx.execute(
                "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
                [],
            )?;
            Err::<(), Error>(Error::UnsupportedPredicate {
                shape: "forced".to_string(),
            })
        })
        .unwrap_err();
        assert!(matches!(err, Error::UnsupportedPredicate { .. }));
        assert!(read_log_since(&conn, "users", 0, 10).unwrap().is_empty());
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
    fn bounded_replay_reports_up_to_date_only_at_consistency_boundary() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = active_users_shape();

        for id in 1..=3 {
            conn.execute(
                "INSERT INTO users (id, name, active) VALUES (?1, ?2, 1)",
                (id, format!("user {id}")),
            )
            .unwrap();
        }

        let first = replay(&conn, &shape, 0, 1).unwrap();
        assert_eq!(first.offset, 1);
        assert!(!first.up_to_date);
        assert_eq!(first.messages.len(), 1);

        let second = replay(&conn, &shape, first.offset, 1).unwrap();
        assert_eq!(second.offset, 2);
        assert!(!second.up_to_date);

        let third = replay(&conn, &shape, second.offset, 1).unwrap();
        assert_eq!(third.offset, 3);
        assert!(third.up_to_date);
    }

    #[test]
    fn replay_page_exposes_shape_cursor_metadata_without_changing_replay_contract() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = active_users_shape();

        for id in 1..=2 {
            conn.execute(
                "INSERT INTO users (id, name, active) VALUES (?1, ?2, 1)",
                (id, format!("user {id}")),
            )
            .unwrap();
        }

        let page = replay_page(&conn, &shape, 0, 1).unwrap();
        assert_eq!(page.cursor.shape_handle, shape.handle());
        assert_eq!(page.cursor.retained_source_offset, 0);
        assert_eq!(page.cursor.source_offset, 1);
        assert_eq!(page.source_offset_start, 0);
        assert_eq!(page.source_offset_end, 1);
        assert!(!page.up_to_date);

        let public_replay = page.replay();
        assert_eq!(public_replay.offset, 1);
        assert_eq!(public_replay.up_to_date, false);
        assert_eq!(public_replay.messages.len(), 1);
    }

    #[test]
    fn semantic_oracle_converges_across_membership_batches_pk_changes_and_compaction() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE todos (
              id INTEGER PRIMARY KEY,
              project_id TEXT NOT NULL,
              title TEXT NOT NULL,
              done BOOLEAN NOT NULL DEFAULT 0,
              rank INTEGER
            );
            ",
        )
        .unwrap();
        install_triggers_auto(&conn, "todos").unwrap();
        let shape = Shape {
            name: "doneP1Todos".to_string(),
            table: "todos".to_string(),
            columns: vec![
                "id".to_string(),
                "project_id".to_string(),
                "title".to_string(),
                "done".to_string(),
                "rank".to_string(),
            ],
            predicate: Predicate::And {
                predicates: vec![
                    Predicate::Eq {
                        column: "project_id".to_string(),
                        value: json!("p1"),
                    },
                    Predicate::Eq {
                        column: "done".to_string(),
                        value: json!(true),
                    },
                ],
            },
            auth_scope: "project:p1".to_string(),
            schema_version: 1,
        };

        conn.execute_batch(
            "
            INSERT INTO todos (id, project_id, title, done, rank) VALUES
              (1, 'p1', 'visible', 1, 10),
              (2, 'p2', 'other project', 1, 20),
              (3, 'p1', 'not done', 0, 30);
            ",
        )
        .unwrap();
        let mut materialized =
            MaterializedShape::from_snapshot(initial_snapshot(&conn, &shape).unwrap());
        assert_eq!(materialized.values(), authoritative_rows(&conn, &shape));

        let operations: &[&str] = &[
            "UPDATE todos SET title='still private' WHERE id=2",
            "UPDATE todos SET project_id='p1' WHERE id=2",
            "UPDATE todos SET title='visible updated' WHERE id=1",
            "UPDATE todos SET done=1 WHERE id=3",
            "UPDATE todos SET project_id='p3' WHERE id=1",
            "DELETE FROM todos WHERE id=1",
        ];
        for operation in operations {
            conn.execute(operation, []).unwrap();
            sync_until_up_to_date(&conn, &shape, &mut materialized, 1);
            assert_eq!(materialized.values(), authoritative_rows(&conn, &shape));
        }

        change_batch(&mut conn, |tx| {
            tx.execute(
                "INSERT INTO todos (id, project_id, title, done, rank) VALUES (4, 'p1', 'batch one', 1, 40)",
                [],
            )?;
            tx.execute(
                "INSERT INTO todos (id, project_id, title, done, rank) VALUES (5, 'p1', 'batch two', 1, 50)",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        let pages = sync_until_up_to_date(&conn, &shape, &mut materialized, 1);
        assert_eq!(
            pages, 1,
            "Electrolite batches must not be split across replay pages"
        );
        assert_eq!(materialized.values(), authoritative_rows(&conn, &shape));

        conn.execute("UPDATE todos SET id=40 WHERE id=4", [])
            .unwrap();
        sync_until_up_to_date(&conn, &shape, &mut materialized, 1);
        assert_eq!(materialized.values(), authoritative_rows(&conn, &shape));

        compact_log_to_last_for_table(&conn, "todos", 4).unwrap();
        conn.execute("UPDATE todos SET rank=NULL WHERE id=5", [])
            .unwrap();
        sync_until_up_to_date(&conn, &shape, &mut materialized, 1);
        assert_eq!(materialized.values(), authoritative_rows(&conn, &shape));
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
    fn compaction_records_retained_offset_even_when_log_is_empty() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = active_users_shape();

        for id in 1..=3 {
            conn.execute(
                "INSERT INTO users (id, name, active) VALUES (?1, ?2, 1)",
                (id, format!("user {id}")),
            )
            .unwrap();
        }

        let stats = compact_log_before(&conn, 3).unwrap();
        assert_eq!(
            stats,
            RetentionStats {
                retained_offset: 3,
                deleted_rows: 3,
            }
        );
        assert_eq!(retained_lower_bound(&conn).unwrap(), 3);

        let err = replay(&conn, &shape, 2, 10).unwrap_err();
        assert!(matches!(
            err,
            Error::ResyncRequired {
                requested_offset: 2,
                retained_offset: 3,
            }
        ));
    }

    #[test]
    fn compaction_can_keep_last_n_log_rows() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);

        for id in 1..=5 {
            conn.execute(
                "INSERT INTO users (id, name, active) VALUES (?1, ?2, 1)",
                (id, format!("user {id}")),
            )
            .unwrap();
        }

        let stats = compact_log_to_last(&conn, 2).unwrap();
        assert_eq!(stats.retained_offset, 3);
        assert_eq!(stats.deleted_rows, 3);
        assert_eq!(retained_lower_bound(&conn).unwrap(), 3);
        assert_eq!(read_log_since(&conn, "users", 3, 10).unwrap().len(), 2);
    }

    #[test]
    fn global_compaction_records_per_table_retention_offsets() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        conn.execute_batch(
            "
            CREATE TABLE projects (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            );
            ",
        )
        .unwrap();
        install_triggers(&conn, "projects", "id").unwrap();
        let users_shape = active_users_shape();
        let projects_shape = Shape {
            name: "projects".to_string(),
            table: "projects".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            predicate: Predicate::All,
            auth_scope: "public".to_string(),
            schema_version: 1,
        };

        conn.execute(
            "INSERT INTO users (id, name, active) VALUES (1, 'Ada', 1)",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO projects (id, name) VALUES (1, 'One')", [])
            .unwrap();
        conn.execute("UPDATE users SET name='Ada Lovelace' WHERE id=1", [])
            .unwrap();

        compact_log_before(&conn, 3).unwrap();
        assert_eq!(retained_lower_bound_for_table(&conn, "users").unwrap(), 3);
        assert_eq!(
            retained_lower_bound_for_table(&conn, "projects").unwrap(),
            2
        );

        let users_err = replay(&conn, &users_shape, 2, 10).unwrap_err();
        assert!(matches!(
            users_err,
            Error::ResyncRequired {
                requested_offset: 2,
                retained_offset: 3,
            }
        ));
        let projects_err = replay(&conn, &projects_shape, 1, 10).unwrap_err();
        assert!(matches!(
            projects_err,
            Error::ResyncRequired {
                requested_offset: 1,
                retained_offset: 2,
            }
        ));
        assert!(replay(&conn, &projects_shape, 2, 10).is_ok());
    }

    #[test]
    fn quiet_tables_are_not_forced_to_resync_by_unrelated_compaction() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        conn.execute_batch(
            "
            CREATE TABLE projects (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            );
            ",
        )
        .unwrap();
        install_triggers(&conn, "projects", "id").unwrap();
        let users_shape = active_users_shape();

        let users_snapshot = initial_snapshot(&conn, &users_shape).unwrap();
        assert_eq!(users_snapshot.offset, 0);
        for id in 1..=5 {
            conn.execute(
                "INSERT INTO projects (id, name) VALUES (?1, ?2)",
                (id, format!("project {id}")),
            )
            .unwrap();
        }

        compact_log_to_last(&conn, 0).unwrap();
        let replayed = replay(&conn, &users_shape, users_snapshot.offset, 10).unwrap();
        assert_eq!(replayed.offset, 0);
        assert!(replayed.up_to_date);
        assert!(replayed.messages.is_empty());
    }

    #[test]
    fn boolean_predicates_are_normalized_for_declared_boolean_columns() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE flags (
              id INTEGER PRIMARY KEY,
              enabled BOOLEAN NOT NULL
            );
            ",
        )
        .unwrap();
        install_triggers(&conn, "flags", "id").unwrap();
        let shape = Shape {
            name: "enabledFlags".to_string(),
            table: "flags".to_string(),
            columns: vec!["id".to_string(), "enabled".to_string()],
            predicate: Predicate::Eq {
                column: "enabled".to_string(),
                value: json!(true),
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        };

        conn.execute("INSERT INTO flags (id, enabled) VALUES (1, 1)", [])
            .unwrap();
        conn.execute("INSERT INTO flags (id, enabled) VALUES (2, 0)", [])
            .unwrap();
        let snapshot = initial_snapshot(&conn, &shape).unwrap();
        assert_eq!(snapshot.rows, vec![json!({"id": 1, "enabled": 1})]);

        conn.execute("UPDATE flags SET enabled=1 WHERE id=2", [])
            .unwrap();
        let replayed = replay(&conn, &shape, snapshot.offset, 10).unwrap();
        assert_eq!(
            replayed.messages,
            vec![ShapeMessage::Insert {
                key: json!({"id": 2}),
                value: json!({"id": 2, "enabled": 1}),
                offset: 3,
            }]
        );
    }

    #[test]
    fn boolean_predicates_are_rejected_for_plain_integer_columns() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        let shape = Shape {
            name: "activeUsersBool".to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "active".to_string()],
            predicate: Predicate::Eq {
                column: "active".to_string(),
                value: json!(true),
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        };

        let err = initial_snapshot(&conn, &shape).unwrap_err();
        assert!(matches!(
            err,
            Error::UnsupportedPredicateValue { ref shape, ref value }
                if shape == "activeUsersBool" && value == &json!(true)
        ));
    }

    #[test]
    fn ambiguous_text_and_numeric_predicate_values_are_rejected() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE typed_values (
              id INTEGER PRIMARY KEY,
              count INTEGER NOT NULL,
              label TEXT NOT NULL
            );
            ",
        )
        .unwrap();
        install_triggers_auto(&conn, "typed_values").unwrap();

        let numeric_string = Shape {
            name: "numericString".to_string(),
            table: "typed_values".to_string(),
            columns: vec!["id".to_string(), "count".to_string(), "label".to_string()],
            predicate: Predicate::Eq {
                column: "count".to_string(),
                value: json!("1"),
            },
            auth_scope: "public".to_string(),
            schema_version: 1,
        };
        let err = initial_snapshot(&conn, &numeric_string).unwrap_err();
        assert!(matches!(
            err,
            Error::UnsupportedPredicateValue { ref shape, ref value }
                if shape == "numericString" && value == &json!("1")
        ));

        let text_number = Shape {
            name: "textNumber".to_string(),
            predicate: Predicate::Eq {
                column: "label".to_string(),
                value: json!(1),
            },
            ..numeric_string
        };
        let err = initial_snapshot(&conn, &text_number).unwrap_err();
        assert!(matches!(
            err,
            Error::UnsupportedPredicateValue { ref shape, ref value }
                if shape == "textNumber" && value == &json!(1)
        ));
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
    fn in_predicate_matches_snapshot_and_replay() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE todos (
              id INTEGER PRIMARY KEY,
              project_id TEXT,
              title TEXT NOT NULL
            );
            ",
        )
        .unwrap();
        install_triggers(&conn, "todos", "id").unwrap();
        let shape = Shape {
            name: "selectedProjectTodos".to_string(),
            table: "todos".to_string(),
            columns: vec![
                "id".to_string(),
                "project_id".to_string(),
                "title".to_string(),
            ],
            predicate: Predicate::In {
                column: "project_id".to_string(),
                values: vec![json!("p1"), json!("p2"), Value::Null],
            },
            auth_scope: "projects:p1,p2,null".to_string(),
            schema_version: 1,
        };

        conn.execute(
            "INSERT INTO todos (id, project_id, title) VALUES (1, 'p1', 'one')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO todos (id, project_id, title) VALUES (2, 'p3', 'three')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO todos (id, project_id, title) VALUES (3, NULL, 'null project')",
            [],
        )
        .unwrap();
        let snapshot = initial_snapshot(&conn, &shape).unwrap();
        assert_eq!(
            snapshot.rows,
            vec![
                json!({"id": 1, "project_id": "p1", "title": "one"}),
                json!({"id": 3, "project_id": null, "title": "null project"}),
            ]
        );

        conn.execute("UPDATE todos SET project_id='p2' WHERE id=2", [])
            .unwrap();
        let replayed = replay(&conn, &shape, snapshot.offset, 10).unwrap();
        assert_eq!(
            replayed.messages,
            vec![ShapeMessage::Insert {
                key: json!({"id": 2}),
                value: json!({"id": 2, "project_id": "p2", "title": "three"}),
                offset: 4,
            }]
        );
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
    fn composite_primary_keys_snapshot_and_replay_with_full_json_key() {
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
        install_triggers_auto(&conn, "memberships").unwrap();
        let shape = Shape {
            name: "memberships".to_string(),
            table: "memberships".to_string(),
            columns: vec![
                "account_id".to_string(),
                "user_id".to_string(),
                "role".to_string(),
            ],
            predicate: Predicate::All,
            auth_scope: "public".to_string(),
            schema_version: 1,
        };

        conn.execute(
            "INSERT INTO memberships (account_id, user_id, role) VALUES (7, 11, 'admin')",
            [],
        )
        .unwrap();
        let snapshot = initial_snapshot(&conn, &shape).unwrap();
        assert_eq!(
            snapshot.key_columns,
            vec!["account_id".to_string(), "user_id".to_string()]
        );
        assert_eq!(
            snapshot.rows,
            vec![json!({"account_id": 7, "user_id": 11, "role": "admin"})]
        );

        conn.execute(
            "UPDATE memberships SET role='member' WHERE account_id=7 AND user_id=11",
            [],
        )
        .unwrap();
        let replayed = replay(&conn, &shape, snapshot.offset, 10).unwrap();
        assert_eq!(
            replayed.messages,
            vec![ShapeMessage::Update {
                key: json!({"account_id": 7, "user_id": 11}),
                value: json!({"account_id": 7, "user_id": 11, "role": "member"}),
                offset: 2,
            }]
        );
    }
}
