//! Tiny test-only HTTP server exposing the Rust engine over Electrolite's
//! HTTP protocol. Used by the cross-language client × engine matrix.
//!
//! Usage:
//!   cargo run --manifest-path engines/rust/Cargo.toml \
//!     --bin electrolite-server -- --port 5103 --db /tmp/x/app.db

use electrolite::{eq, gt, AuthContext, BuildContext, Electrolite, Predicate, ShapeDef};
use serde_json::{json, Value as Json};
use std::env;
use std::io::Read;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tiny_http::{Header, Method, Response, Server};

fn main() {
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
        "#,
    )
    .unwrap();
    app.install_triggers("todos").unwrap();
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
                    _ => {}
                }
            }

            respond(request, 404, &json!({"error": "not_found"}));
        });
    }
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
