use electrolite_core::Shape;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, params_from_iter};
use serde::Deserialize;
use serde_json::Value;
use std::sync::{Arc, Condvar, Mutex};

#[napi]
pub struct NativeElectrolite {
    pool: Arc<ConnectionPool>,
}

#[napi]
impl NativeElectrolite {
    #[napi(constructor)]
    pub fn new(db_path: String, pool_size: Option<u32>) -> Self {
        Self {
            pool: Arc::new(ConnectionPool::new(
                db_path,
                pool_size.unwrap_or(1) as usize,
            )),
        }
    }

    #[napi]
    pub fn install_triggers_auto(&self, table: String) -> Result<String> {
        let conn = self.pool.get()?;
        let watched = electrolite_sqlite::install_triggers_auto(&conn, &table).map_err(to_napi)?;
        serde_json::to_string(&serde_json::json!({
            "table": watched.table,
            "key_columns": watched.pk_columns,
            "columns": watched.columns,
        }))
        .map_err(to_napi)
    }

    #[napi]
    pub fn install_triggers(&self, table: String, pk_column: String) -> Result<String> {
        let conn = self.pool.get()?;
        let watched =
            electrolite_sqlite::install_triggers(&conn, &table, &pk_column).map_err(to_napi)?;
        serde_json::to_string(&serde_json::json!({
            "table": watched.table,
            "key_columns": watched.pk_columns,
            "columns": watched.columns,
        }))
        .map_err(to_napi)
    }

    #[napi]
    pub fn snapshot(&self, shape_json: String) -> Result<String> {
        let conn = self.pool.get()?;
        let shape = parse_shape(&shape_json)?;
        let snapshot = electrolite_sqlite::initial_snapshot(&conn, &shape).map_err(to_napi)?;
        serde_json::to_string(&snapshot).map_err(to_napi)
    }

    #[napi]
    pub fn replay(&self, shape_json: String, offset: i64, limit: Option<i64>) -> Result<String> {
        let conn = self.pool.get()?;
        let shape = parse_shape(&shape_json)?;
        let replay = electrolite_sqlite::replay(&conn, &shape, offset, limit.unwrap_or(1000))
            .map_err(to_napi)?;
        serde_json::to_string(&replay).map_err(to_napi)
    }

    #[napi]
    pub fn shape_handle(&self, shape_json: String) -> Result<String> {
        let conn = self.pool.get()?;
        let shape = parse_shape(&shape_json)?;
        electrolite_sqlite::shape_handle(&conn, &shape).map_err(to_napi)
    }

    #[napi]
    pub fn high_water_mark(&self) -> Result<i64> {
        let conn = self.pool.get()?;
        electrolite_sqlite::high_water_mark(&conn).map_err(to_napi)
    }

    #[napi]
    pub fn compact_log_to_last_for_table(
        &self,
        table_name: String,
        keep_last: i64,
    ) -> Result<String> {
        let conn = self.pool.get()?;
        let stats =
            electrolite_sqlite::compact_log_to_last_for_table(&conn, &table_name, keep_last)
                .map_err(to_napi)?;
        serde_json::to_string(&serde_json::json!({
            "retained_offset": stats.retained_offset,
            "deleted_rows": stats.deleted_rows,
        }))
        .map_err(to_napi)
    }

    #[napi]
    pub fn execute_batch(&self, sql: String) -> Result<()> {
        let conn = self.pool.get()?;
        conn.execute_batch(&sql).map_err(to_napi)
    }

    #[napi]
    pub fn execute(&self, sql: String, params_json: Option<String>) -> Result<u32> {
        let conn = self.pool.get()?;
        execute_on_connection(&conn, &sql, params_json.as_deref()).map(|rows| rows as u32)
    }

    #[napi]
    pub fn write_batch(&self, statements_json: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let statements =
            serde_json::from_str::<Vec<NativeStatement>>(&statements_json).map_err(to_napi)?;
        let statements = statements
            .into_iter()
            .map(|statement| {
                let params = statement.params.unwrap_or(Value::Array(Vec::new()));
                Ok((statement.sql, json_params(params)?))
            })
            .collect::<Result<Vec<_>>>()?;
        electrolite_sqlite::change_batch(&mut conn, |tx| {
            for (sql, params) in statements {
                tx.execute(&sql, params_from_iter(params.iter()))?;
            }
            Ok(())
        })
        .map_err(to_napi)
    }
}

struct ConnectionPool {
    db_path: String,
    max_size: usize,
    inner: Mutex<ConnectionPoolInner>,
    available: Condvar,
}

#[derive(Default)]
struct ConnectionPoolInner {
    idle: Vec<Connection>,
    open: usize,
}

impl ConnectionPool {
    fn new(db_path: String, max_size: usize) -> Self {
        Self {
            db_path,
            max_size: max_size.max(1),
            inner: Mutex::new(ConnectionPoolInner::default()),
            available: Condvar::new(),
        }
    }

    fn get(self: &Arc<Self>) -> Result<PooledConnection> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| Error::from_reason("connection pool mutex is poisoned"))?;
        loop {
            if let Some(conn) = inner.idle.pop() {
                return Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self.clone(),
                });
            }
            if inner.open < self.max_size {
                inner.open += 1;
                drop(inner);
                match Connection::open(&self.db_path) {
                    Ok(conn) => {
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: self.clone(),
                        });
                    }
                    Err(error) => {
                        let mut inner = self
                            .inner
                            .lock()
                            .map_err(|_| Error::from_reason("connection pool mutex is poisoned"))?;
                        inner.open = inner.open.saturating_sub(1);
                        self.available.notify_one();
                        return Err(to_napi(error));
                    }
                }
            }
            inner = self
                .available
                .wait(inner)
                .map_err(|_| Error::from_reason("connection pool mutex is poisoned"))?;
        }
    }

    fn put(&self, conn: Connection) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.idle.push(conn);
            self.available.notify_one();
        }
    }
}

struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<ConnectionPool>,
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("pooled connection is present")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("pooled connection is present")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.put(conn);
        }
    }
}

#[derive(Debug, Deserialize)]
struct NativeStatement {
    sql: String,
    params: Option<Value>,
}

fn parse_shape(shape_json: &str) -> Result<Shape> {
    serde_json::from_str(shape_json).map_err(to_napi)
}

fn execute_on_connection(conn: &Connection, sql: &str, params_json: Option<&str>) -> Result<usize> {
    let params = params_json
        .map(serde_json::from_str::<Value>)
        .transpose()
        .map_err(to_napi)?
        .unwrap_or(Value::Array(Vec::new()));
    let params = json_params(params)?;
    conn.execute(sql, params_from_iter(params.iter()))
        .map_err(to_napi)
}

fn json_params(value: Value) -> Result<Vec<SqlValue>> {
    let Value::Array(values) = value else {
        return Err(Error::from_reason("SQL params must be a JSON array"));
    };
    values.into_iter().map(json_to_sql_value).collect()
}

fn json_to_sql_value(value: Value) -> Result<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                SqlValue::Integer(value)
            } else if let Some(value) = value.as_u64().and_then(|value| i64::try_from(value).ok()) {
                SqlValue::Integer(value)
            } else if let Some(value) = value.as_f64() {
                SqlValue::Real(value)
            } else {
                return Err(Error::from_reason("unsupported JSON number parameter"));
            }
        }
        Value::String(value) => SqlValue::Text(value),
        Value::Array(_) | Value::Object(_) => {
            return Err(Error::from_reason(
                "SQL params cannot contain arrays or objects",
            ));
        }
    })
}

fn to_napi(error: impl std::fmt::Display) -> Error {
    Error::from_reason(error.to_string())
}
