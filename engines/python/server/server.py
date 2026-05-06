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

from electrolite import (  # noqa: E402
    and_,
    create_electrolite,
    eq,
    gt,
    in_list,
    predicate_matches,
    shape,
)


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
                accept = self.headers.get("accept", "")
                if "text/event-stream" in accept:
                    self._stream_sse(parsed)
                    return
                status, body = app.handle(
                    parsed.path,
                    parsed.query,
                    context={"projects": {"p1", "p2"}},
                )
                self._send(status, body)
                return
            self._send(404, {"error": "not_found"})

        def _stream_sse(self, parsed):
            # Send headers for an event-stream response.
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("cache-control", "no-cache")
            self.send_header("connection", "keep-alive")
            self.send_header("access-control-allow-origin", "*")
            self.end_headers()

            from urllib.parse import parse_qs as _pq
            from threading import Event
            qd = _pq(parsed.query)
            try:
                offset = int(qd.get("offset", ["-1"])[0])
            except ValueError:
                self._write_event("error", {"error": "bad_request"})
                return
            ctx = {"projects": {"p1", "p2"}}

            # Initial snapshot or replay.
            status, body = app.handle(parsed.path, parsed.query, context=ctx)
            if status != 200:
                self._write_event("error", body)
                return
            kind = "snapshot" if offset < 0 else "replay"
            self._write_event(kind, body)
            offset = body.get("offset", offset)

            # Loop: wait for change, replay, send. On disconnect, break.
            log_id = body.get("log_id", "")
            shape_handle = body.get("shape_handle", "")
            while True:
                # Build a query for the next replay.
                next_query = (
                    f"offset={offset}&log_id={log_id}&shape_handle={shape_handle}&live=true"
                )
                status, body = app.handle(parsed.path, next_query, context=ctx)
                if status != 200:
                    self._write_event("error", body)
                    return
                if body.get("messages"):
                    self._write_event("replay", body)
                    offset = body.get("offset", offset)
                # If client disconnected, write will fail.
                try:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return

        def _write_event(self, event, data):
            try:
                payload = json.dumps(data).encode()
                frame = b"event: " + event.encode() + b"\ndata: " + payload + b"\n\n"
                self.wfile.write(frame)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                raise

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
            if self.path == "/_test/match-predicate":
                pred = payload["predicate"]
                rows = payload["rows"]
                matched_ids = [
                    row["id"] for row in rows if predicate_matches(pred, row)
                ]
                self._send(200, {"matched_ids": matched_ids})
                return
            self._send(404, {"error": "not_found"})

    return Handler


def main():
    if os.environ.get("ELECTROLITE_TEST_SERVER") != "1":
        sys.stderr.write(
            "engines/python/server/server.py is a test-only HTTP server "
            "with an unauthenticated /_test/exec endpoint. Set "
            "ELECTROLITE_TEST_SERVER=1 to launch it.\n"
        )
        sys.exit(1)
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
