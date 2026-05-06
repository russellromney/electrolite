"""Tiny test-only HTTP server that exposes the Python engine over the
Electrolite protocol so cross-language client tests can drive a real
browser ShapeClient against it.

Routes:
  GET  /electrolite/v1/...      forwarded to engine.handle()
  POST /_test/exec              { "sql": "...", "args": [...] }
  POST /_test/write_batch       { "statements": [["sql", [args]], ...] }

Usage:
  python3 -m server.server --port 5101 --db /tmp/electrolite-x/app.db
"""

import argparse
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
ENGINE_DIR = os.path.dirname(HERE)
sys.path.insert(0, ENGINE_DIR)

from electrolite import create_electrolite, eq, gt, in_list, shape  # noqa: E402


def build_app(db_path: str):
    app = create_electrolite(
        db_path,
        live_timeout_ms=2_000,
        shapes={
            "projectTodos": shape(
                table="todos",
                columns=["id", "project_id", "title", "done"],
                params=["project_id"],
                where=lambda ctx: eq("project_id", ctx["params"]["project_id"]),
                scope=lambda ctx: "project:" + ctx["params"]["project_id"],
                authorize=lambda ctx: ctx["params"]["project_id"] in ctx["context"]["projects"],
            ),
            "highIds": shape(
                table="todos",
                columns=["id", "project_id", "title", "done"],
                where=lambda ctx: gt("id", 1),
            ),
        },
    )
    app.execute_batch(
        """
        CREATE TABLE IF NOT EXISTS todos (
          id INTEGER PRIMARY KEY,
          project_id TEXT NOT NULL,
          title TEXT NOT NULL,
          done INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    app.install_triggers("todos")
    return app


def make_handler(app):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args, **kwargs):
            return  # silence access logs in tests

        def _send(self, status, body):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.send_header("access-control-allow-origin", "*")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path.startswith("/electrolite/"):
                status, body = app.handle(
                    parsed.path,
                    parsed.query,
                    context={"projects": {"p1", "p2"}},
                )
                self._send(status, body)
                return
            self._send(404, {"error": "not_found"})

        def do_POST(self):
            length = int(self.headers.get("content-length") or 0)
            data = self.rfile.read(length) if length else b""
            try:
                payload = json.loads(data) if data else {}
            except Exception:
                self._send(400, {"error": "bad_json"})
                return
            if self.path == "/_test/exec":
                app.execute(payload["sql"], payload.get("args") or [])
                self._send(200, {"ok": True})
                return
            if self.path == "/_test/write_batch":
                app.write_batch([(s[0], s[1]) for s in payload["statements"]])
                self._send(200, {"ok": True})
                return
            if self.path == "/_test/seed":
                app.execute_batch(payload["sql"])
                self._send(200, {"ok": True})
                return
            self._send(404, {"error": "not_found"})

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    app = build_app(args.db)
    handler_cls = make_handler(app)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler_cls)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    # readiness signal for the test runner
    sys.stdout.write(f"electrolite-server listening on {args.port}\n")
    sys.stdout.flush()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
