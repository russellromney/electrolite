use electrolite_core::Shape;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, params_from_iter};
use serde::Deserialize;
use serde_json::Value;

#[napi]
pub struct NativeElectrolite {
    db_path: String,
}

#[napi]
impl NativeElectrolite {
    #[napi(constructor)]
    pub fn new(db_path: String) -> Self {
        Self { db_path }
    }

    #[napi]
    pub fn install_triggers_auto(&self, table: String) -> Result<String> {
        let conn = self.open()?;
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
        let conn = self.open()?;
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
        let conn = self.open()?;
        let shape = parse_shape(&shape_json)?;
        let snapshot = electrolite_sqlite::initial_snapshot(&conn, &shape).map_err(to_napi)?;
        serde_json::to_string(&snapshot).map_err(to_napi)
    }

    #[napi]
    pub fn replay(&self, shape_json: String, offset: i64, limit: Option<i64>) -> Result<String> {
        let conn = self.open()?;
        let shape = parse_shape(&shape_json)?;
        let replay = electrolite_sqlite::replay(&conn, &shape, offset, limit.unwrap_or(1000))
            .map_err(to_napi)?;
        serde_json::to_string(&replay).map_err(to_napi)
    }

    #[napi]
    pub fn shape_handle(&self, shape_json: String) -> Result<String> {
        Ok(parse_shape(&shape_json)?.handle())
    }

    #[napi]
    pub fn high_water_mark(&self) -> Result<i64> {
        let conn = self.open()?;
        electrolite_sqlite::high_water_mark(&conn).map_err(to_napi)
    }

    #[napi]
    pub fn compact_log_to_last_for_table(
        &self,
        table_name: String,
        keep_last: i64,
    ) -> Result<String> {
        let conn = self.open()?;
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
        let conn = self.open()?;
        conn.execute_batch(&sql).map_err(to_napi)
    }

    #[napi]
    pub fn execute(&self, sql: String, params_json: Option<String>) -> Result<u32> {
        let conn = self.open()?;
        execute_on_connection(&conn, &sql, params_json.as_deref()).map(|rows| rows as u32)
    }

    #[napi]
    pub fn write_batch(&self, statements_json: String) -> Result<()> {
        let mut conn = self.open()?;
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

    fn open(&self) -> Result<Connection> {
        Connection::open(&self.db_path).map_err(to_napi)
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
