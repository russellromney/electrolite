//! Tiny experimental Electrolite engine for Rust.
//!
//! Mirrors the protocol shape of the Python and Node engines:
//! install SQLite triggers, take a snapshot, replay logical changes,
//! and run explicit write batches with a shared batch_id.

use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension};
use serde_json::{json, Map, Value as Json};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum Predicate {
    All,
    Eq { column: String, value: Json },
}

#[derive(Clone, Debug)]
pub struct Shape {
    pub table: String,
    pub columns: Vec<String>,
    pub predicate: Predicate,
}

pub struct Electrolite {
    db: Mutex<Connection>,
    cv: Condvar,
    notify_lock: Mutex<u64>,
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
pub enum Error {
    Sqlite(rusqlite::Error),
    Bad(String),
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Sqlite(e)
    }
}

impl Electrolite {
    pub fn open(path: &str) -> Result<Self, Error> {
        let db = Connection::open(path)?;
        let me = Self {
            db: Mutex::new(db),
            cv: Condvar::new(),
            notify_lock: Mutex::new(0),
            live_timeout: Duration::from_millis(20_000),
        };
        me.bootstrap()?;
        Ok(me)
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
        Ok(())
    }

    pub fn execute(&self, sql: &str, args: &[Value]) -> Result<usize, Error> {
        let n = {
            let db = self.db.lock().unwrap();
            db.execute(sql, params_from_iter(args.iter()))?
        };
        self.notify();
        Ok(n)
    }

    pub fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        {
            let db = self.db.lock().unwrap();
            db.execute_batch(sql)?;
        }
        self.notify();
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
        {
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
        }
        self.notify();
        Ok(())
    }

    pub fn snapshot(&self, shape: &Shape) -> Result<Snapshot, Error> {
        let info = self.watched_info(&shape.table)?;
        let normalized = normalize(shape);
        let (where_sql, args) = compile_predicate(&shape.predicate);
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
        let _info = self.watched_info(&shape.table)?;
        let normalized = normalize(shape);
        let db = self.db.lock().unwrap();
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
            messages.extend(messages_for(&shape.predicate, r));
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
        let lock = self.notify_lock.lock().unwrap();
        let _ = self.cv.wait_timeout(lock, self.live_timeout).unwrap();
    }

    fn notify(&self) {
        let mut g = self.notify_lock.lock().unwrap();
        *g = g.wrapping_add(1);
        self.cv.notify_all();
    }

    fn inspect_table(&self, table: &str) -> Result<TableInfo, Error> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare(&format!("PRAGMA table_info({})", quote_string(table)))?;
        let mut cols: Vec<(i64, String, i64)> = stmt
            .query_map([], |r| {
                let cid: i64 = r.get(0)?;
                let name: String = r.get(1)?;
                let pk: i64 = r.get(5)?;
                Ok((cid, name, pk))
            })?
            .collect::<Result<_, _>>()?;
        if cols.is_empty() {
            return Err(Error::Bad(format!("table {table} does not exist")));
        }
        cols.sort_by_key(|c| c.0);
        let columns = cols.iter().map(|c| c.1.clone()).collect();
        let mut pks: Vec<(i64, String)> =
            cols.into_iter().filter(|c| c.2 > 0).map(|c| (c.2, c.1)).collect();
        pks.sort_by_key(|c| c.0);
        let pk = pks.into_iter().map(|c| c.1).collect();
        Ok(TableInfo { columns, pk })
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
        })
    }
}

struct TableInfo {
    columns: Vec<String>,
    pk: Vec<String>,
}

#[derive(Clone)]
struct LogRow {
    seq: i64,
    batch_id: String,
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

fn normalize(shape: &Shape) -> Json {
    let mut cols = shape.columns.clone();
    cols.sort();
    let pred = match &shape.predicate {
        Predicate::All => json!({"type": "all"}),
        Predicate::Eq { column, value } => json!({"type": "eq", "column": column, "value": value}),
    };
    json!({
        "table": shape.table,
        "columns": cols,
        "predicate": pred,
        "auth_scope": "",
        "schema_version": 1,
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
    // Minimal SHA-256 implementation to avoid pulling a crypto crate.
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
