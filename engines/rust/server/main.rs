//! Tiny test-only HTTP server exposing the Rust engine over Electrolite's
//! HTTP protocol. Used by the cross-language client × engine matrix.
//!
//! Usage:
//!   cargo run --manifest-path engines/rust/Cargo.toml \
//!     --bin electrolite-server -- --port 5103 --db /tmp/x/app.db

use electrolite::{
    eq, gt, predicate_from_json, predicate_matches_row, AuthContext, BuildContext, Electrolite,
    Predicate, ShapeDef,
};
use serde_json::{json, Value as Json};
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tiny_http::{Header, Method, Response, Server};

fn main() {
    if env::var("ELECTROLITE_TEST_SERVER").as_deref() != Ok("1") {
        eprintln!(
            "engines/rust/server is a test-only HTTP server with an \
             unauthenticated /_test/exec endpoint. Set \
             ELECTROLITE_TEST_SERVER=1 to launch it."
        );
        std::process::exit(1);
    }
    let mut port: u16 = 0;
    let mut db_path = String::new();
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--port" => port = args.next().unwrap().parse().unwrap(),
            "--db" => db_path = args.next().unwrap(),
            _ => panic!("unknown arg: {}", a),
        }
    }
    if port == 0 || db_path.is_empty() {
        panic!("--port and --db required");
    }

    let mut app = Electrolite::open(&db_path).unwrap();
    app.live_timeout = Duration::from_millis(2_000);
    app.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS todos (
          id INTEGER PRIMARY KEY,
          project_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS feature_flags (
          id INTEGER PRIMARY KEY,
          enabled BOOLEAN NOT NULL DEFAULT 0
        );
        "#,
    )
    .unwrap();
    app.install_triggers("todos").unwrap();
    app.install_triggers("feature_flags").unwrap();
    app.add_shape(
        "projectTodos",
        ShapeDef::new(
            "todos",
            vec![
                "id".into(),
                "project_id".into(),
                "title".into(),
                "done".into(),
            ],
        )
        .params(vec!["project_id".into()])
        .where_fn(|ctx: &BuildContext| eq("project_id", json!(ctx.params["project_id"].clone())))
        .scope_fn(|ctx: &BuildContext| format!("project:{}", ctx.params["project_id"]))
        .authorize_fn(|ctx: &AuthContext| {
            let arr = ctx.context.get("projects").and_then(|v| v.as_array());
            match arr {
                Some(arr) => arr.iter().any(|p| p.as_str() == Some(&ctx.params["project_id"])),
                None => false,
            }
        }),
    );
    app.add_shape(
        "highIds",
        ShapeDef::new(
            "todos",
            vec![
                "id".into(),
                "project_id".into(),
                "title".into(),
                "done".into(),
            ],
        )
        .where_fn(|_| gt("id", json!(1))),
    );
    // Boolean coercion proof: BOOLEAN column + true predicate.
    app.add_shape(
        "enabledFlags",
        ShapeDef::new(
            "feature_flags",
            vec!["id".into(), "enabled".into()],
        )
        .where_fn(|_| eq("enabled", json!(true))),
    );
    // Range-null proof: every engine must reject with 400.
    app.add_shape(
        "bogusGt",
        ShapeDef::new(
            "todos",
            vec![
                "id".into(),
                "project_id".into(),
                "title".into(),
                "done".into(),
            ],
        )
        .where_fn(|_| gt("id", json!(null))),
    );

    let app = Arc::new(app);
    let server = Server::http(format!("127.0.0.1:{port}")).unwrap();
    println!("electrolite-server listening on {port}");

    for mut request in server.incoming_requests() {
        let app = Arc::clone(&app);
        thread::spawn(move || {
            let url = request.url().to_string();
            let (path, query) = match url.split_once('?') {
                Some((p, q)) => (p.to_string(), q.to_string()),
                None => (url, String::new()),
            };

            if path.starts_with("/electrolite/") && request.method() == &Method::Get {
                let context = json!({"projects": ["p1", "p2"]});
                let accepts_sse = request
                    .headers()
                    .iter()
                    .any(|h| {
                        h.field.as_str().as_str().eq_ignore_ascii_case("accept")
                            && h.value.as_str().contains("text/event-stream")
                    });
                if accepts_sse {
                    stream_sse(request, app, path, query, context);
                    return;
                }
                let (status, body) = app.handle(&path, &query, &context);
                respond(request, status, &body);
                return;
            }

            if request.method() == &Method::Post && path.starts_with("/_test/") {
                let mut buf = String::new();
                let _ = request.as_reader().read_to_string(&mut buf);
                let payload: Json = serde_json::from_str(&buf).unwrap_or(json!({}));
                match path.as_str() {
                    "/_test/exec" => {
                        let sql = payload["sql"].as_str().unwrap_or("");
                        let args = payload["args"].as_array().cloned().unwrap_or_default();
                        let values: Vec<rusqlite::types::Value> =
                            args.iter().map(json_to_sql).collect();
                        app.execute(sql, &values).unwrap();
                        respond(request, 200, &json!({"ok": true}));
                        return;
                    }
                    "/_test/write_batch" => {
                        let stmts = payload["statements"].as_array().cloned().unwrap_or_default();
                        let mut prepared: Vec<(&str, Vec<rusqlite::types::Value>)> = Vec::new();
                        let owned: Vec<(String, Vec<rusqlite::types::Value>)> = stmts
                            .into_iter()
                            .map(|s| {
                                let arr = s.as_array().unwrap();
                                let sql = arr[0].as_str().unwrap().to_string();
                                let args: Vec<rusqlite::types::Value> = arr[1]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .map(json_to_sql)
                                    .collect();
                                (sql, args)
                            })
                            .collect();
                        for (sql, args) in &owned {
                            prepared.push((sql.as_str(), args.clone()));
                        }
                        app.write_batch(&prepared).unwrap();
                        respond(request, 200, &json!({"ok": true}));
                        return;
                    }
                    "/_test/seed" => {
                        let sql = payload["sql"].as_str().unwrap_or("");
                        app.execute_batch(sql).unwrap();
                        respond(request, 200, &json!({"ok": true}));
                        return;
                    }
                    "/_test/match-predicate" => {
                        let predicate = match predicate_from_json(&payload["predicate"]) {
                            Ok(p) => p,
                            Err(e) => {
                                respond(request, 400, &json!({"error": e}));
                                return;
                            }
                        };
                        let rows = payload["rows"].as_array().cloned().unwrap_or_default();
                        let matched: Vec<&Json> = rows
                            .iter()
                            .filter(|row| predicate_matches_row(&predicate, row))
                            .collect();
                        let ids: Vec<Json> = matched
                            .into_iter()
                            .filter_map(|r| r.get("id").cloned())
                            .collect();
                        respond(request, 200, &json!({"matched_ids": ids}));
                        return;
                    }
                    _ => {}
                }
            }

            respond(request, 404, &json!({"error": "not_found"}));
        });
    }
}

/// Stream SSE events to the client by taking control of the
/// underlying TCP writer. tiny_http does not push streamed Read
/// data eagerly enough for SSE (it buffers under 32 KiB), so the
/// SSE branch writes the HTTP response by hand.
fn stream_sse(
    request: tiny_http::Request,
    app: Arc<electrolite::Electrolite>,
    path: String,
    query: String,
    context: Json,
) {
    let mut writer = request.into_writer();
    if writer
        .write_all(
            b"HTTP/1.1 200 OK\r\n\
              content-type: text/event-stream\r\n\
              cache-control: no-cache\r\n\
              access-control-allow-origin: *\r\n\
              \r\n",
        )
        .is_err()
    {
        return;
    }
    let _ = writer.flush();

    // Initial snapshot or replay.
    let (status, body) = app.handle(&path, &query, &context);
    if status != 200 {
        let _ = writer.write_all(&sse_frame("error", &body));
        let _ = writer.flush();
        return;
    }
    let kind = if query
        .split('&')
        .any(|p| p.starts_with("offset=") && p != "offset=-1")
    {
        "replay"
    } else {
        "snapshot"
    };
    if writer.write_all(&sse_frame(kind, &body)).is_err() {
        return;
    }
    let _ = writer.flush();

    let mut offset = body.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
    let log_id = body
        .get("log_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let shape_handle = body
        .get("shape_handle")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    loop {
        let q = format!(
            "offset={}&log_id={}&shape_handle={}&live=true",
            offset, log_id, shape_handle
        );
        let (status, body) = app.handle(&path, &q, &context);
        if status != 200 {
            let _ = writer.write_all(&sse_frame("error", &body));
            let _ = writer.flush();
            return;
        }
        let has_msg = body
            .get("messages")
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        if has_msg {
            if writer.write_all(&sse_frame("replay", &body)).is_err() {
                return;
            }
            let _ = writer.flush();
            offset = body.get("offset").and_then(|v| v.as_i64()).unwrap_or(offset);
        }
        // Heartbeat doubles as a disconnect probe.
        if writer.write_all(b": ping\n\n").is_err() {
            return;
        }
        if writer.flush().is_err() {
            return;
        }
    }
}

fn sse_frame(event: &str, body: &Json) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"event: ");
    out.extend_from_slice(event.as_bytes());
    out.extend_from_slice(b"\ndata: ");
    out.extend_from_slice(serde_json::to_string(body).unwrap().as_bytes());
    out.extend_from_slice(b"\n\n");
    out
}

fn json_to_sql(v: &Json) -> rusqlite::types::Value {
    match v {
        Json::Null => rusqlite::types::Value::Null,
        Json::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else {
                rusqlite::types::Value::Real(n.as_f64().unwrap_or(0.0))
            }
        }
        Json::String(s) => rusqlite::types::Value::Text(s.clone()),
        _ => rusqlite::types::Value::Text(v.to_string()),
    }
}

fn respond(request: tiny_http::Request, status: u16, body: &Json) {
    let payload = serde_json::to_string(body).unwrap();
    let response = Response::from_string(payload)
        .with_status_code(status as i32)
        .with_header(Header::from_bytes(&b"content-type"[..], &b"application/json"[..]).unwrap())
        .with_header(Header::from_bytes(&b"access-control-allow-origin"[..], &b"*"[..]).unwrap());
    let _ = request.respond(response);
}
