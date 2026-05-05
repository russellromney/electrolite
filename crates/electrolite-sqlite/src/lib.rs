use electrolite_core::{LogOp, LogRow};
use rusqlite::Connection;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("table {table:?} has no primary-key column {pk_column:?}")]
    MissingPrimaryKey { table: String, pk_column: String },
    #[error("table {table:?} has no columns")]
    EmptyTable { table: String },
    #[error("unknown electrolite log operation {0:?}")]
    InvalidLogOp(String),
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
    Ok(())
}

pub fn inspect_table(conn: &Connection, table: &str, pk_column: &str) -> Result<WatchedTable> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_string(table)))?;
    let mut rows = stmt.query([])?;
    let mut columns = Vec::new();
    let mut has_pk = false;

    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let pk: i64 = row.get(5)?;
        if name == pk_column && pk > 0 {
            has_pk = true;
        }
        columns.push(name);
    }

    if columns.is_empty() {
        return Err(Error::EmptyTable {
            table: table.to_string(),
        });
    }
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

pub fn install_triggers(conn: &Connection, table: &str, pk_column: &str) -> Result<WatchedTable> {
    bootstrap(conn)?;
    let watched = inspect_table(conn, table, pk_column)?;
    let table_ident = quote_ident(&watched.table);
    let trigger_prefix = trigger_prefix(&watched.table);
    let pk_new = row_json_expr("NEW", &[watched.pk_column.clone()]);
    let pk_old = row_json_expr("OLD", &[watched.pk_column.clone()]);
    let new_row = row_json_expr("NEW", &watched.columns);
    let old_row = row_json_expr("OLD", &watched.columns);
    let table_lit = quote_string(&watched.table);

    conn.execute_batch(&format!(
        "
        CREATE TRIGGER IF NOT EXISTS {insert_trigger}
        AFTER INSERT ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (table_name, op, pk_json, old_json, new_json)
          VALUES ({table_lit}, 'insert', {pk_new}, NULL, {new_row});
        END;

        CREATE TRIGGER IF NOT EXISTS {update_trigger}
        AFTER UPDATE ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (table_name, op, pk_json, old_json, new_json)
          VALUES ({table_lit}, 'update', {pk_new}, {old_row}, {new_row});
        END;

        CREATE TRIGGER IF NOT EXISTS {delete_trigger}
        AFTER DELETE ON {table_ident}
        BEGIN
          INSERT INTO _electrolite_log (table_name, op, pk_json, old_json, new_json)
          VALUES ({table_lit}, 'delete', {pk_old}, {old_row}, NULL);
        END;
        ",
        insert_trigger = quote_ident(&format!("{trigger_prefix}_ai")),
        update_trigger = quote_ident(&format!("{trigger_prefix}_au")),
        delete_trigger = quote_ident(&format!("{trigger_prefix}_ad")),
    ))?;

    Ok(watched)
}

pub fn read_log_since(conn: &Connection, offset: i64, limit: i64) -> Result<Vec<LogRow>> {
    let mut stmt = conn.prepare(
        "
        SELECT seq, table_name, op, pk_json, old_json, new_json, created_at
        FROM _electrolite_log
        WHERE seq > ?1
        ORDER BY seq ASC
        LIMIT ?2
        ",
    )?;
    let rows = stmt.query_map([offset, limit], |row| {
        let pk_json: String = row.get(3)?;
        let old_json: Option<String> = row.get(4)?;
        let new_json: Option<String> = row.get(5)?;
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            pk_json,
            old_json,
            new_json,
            row.get::<_, i64>(6)?,
        ))
    })?;

    let mut out = Vec::new();
    for row in rows {
        let (seq, table_name, op_text, pk_json, old_json, new_json, created_at) = row?;
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

fn row_json_expr(prefix: &str, columns: &[String]) -> String {
    let parts = columns.iter().flat_map(|column| {
        [
            quote_string(column),
            format!("{prefix}.{}", quote_ident(column)),
        ]
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

#[cfg(test)]
mod tests {
    use super::*;
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

        let rows = read_log_since(&conn, 0, 10).unwrap();
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

        let rows = read_log_since(&conn, 0, 10).unwrap();
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

        let rows = read_log_since(&conn1, 0, 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].new_json,
            Some(json!({"id": 1, "name": "Ada", "active": 1}))
        );
    }
}
