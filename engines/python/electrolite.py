from __future__ import annotations

import hashlib
import builtins
import json
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Union
from urllib.parse import parse_qs


Predicate = dict[str, Any]


def all() -> Predicate:
    return {"type": "all"}


def eq(column: str, value: Any) -> Predicate:
    return {"type": "eq", "column": column, "value": value}


def gt(column: str, value: Any) -> Predicate:
    return {"type": "gt", "column": column, "value": value}


def lt(column: str, value: Any) -> Predicate:
    return {"type": "lt", "column": column, "value": value}


def gte(column: str, value: Any) -> Predicate:
    return {"type": "gte", "column": column, "value": value}


def lte(column: str, value: Any) -> Predicate:
    return {"type": "lte", "column": column, "value": value}


def in_list(column: str, values: list[Any]) -> Predicate:
    return {"type": "in", "column": column, "values": values}


def and_(*predicates: Predicate) -> Predicate:
    return {"type": "and", "predicates": list(predicates)}


def or_(*predicates: Predicate) -> Predicate:
    return {"type": "or", "predicates": list(predicates)}


def not_(predicate: Predicate) -> Predicate:
    return {"type": "not", "predicate": predicate}


_RANGE_OPS = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<="}


@dataclass
class Shape:
    table: str
    columns: list[str]
    params: Optional[list[str]] = None
    where: Optional[Callable[[dict[str, Any]], Predicate]] = None
    authorize: Optional[Callable[[dict[str, Any]], bool]] = None
    scope: Optional[Union[str, Callable[[dict[str, Any]], str]]] = None
    schema_version: int = 1


def shape(**kwargs: Any) -> Shape:
    return Shape(**kwargs)


class Electrolite:
    def __init__(
        self,
        db_path: str,
        *,
        shapes: Optional[dict[str, Shape]] = None,
        prefix: str = "/electrolite/v1",
        replay_limit: int = 1000,
        live_timeout_ms: int = 20_000,
    ) -> None:
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.shapes = shapes or {}
        self.prefix = prefix.rstrip("/")
        self.replay_limit = replay_limit
        self.live_timeout_ms = live_timeout_ms
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)
        self._stopped = False
        self.bootstrap()

    def install_triggers(self, table: str) -> dict[str, Any]:
        with self._lock:
            info = self._inspect_table(table)
            if not info["pk_columns"]:
                raise ValueError(f"table {table} must have a primary key")
            self.bootstrap()
            self.db.execute(
                """
                INSERT INTO _electrolite_watched_tables (table_name, pk_columns)
                VALUES (?, ?)
                ON CONFLICT(table_name) DO UPDATE SET pk_columns = excluded.pk_columns
                """,
                (table, json.dumps(info["pk_columns"])),
            )
            self._install_trigger_sql(info)
            self.db.commit()
            return {
                "table": table,
                "key_columns": info["pk_columns"],
                "columns": info["columns"],
            }

    def shutdown(self) -> None:
        """Wake any live waiters then close the SQLite connection.

        Sets `self._stopped` so any in-flight `handle()` call exits
        cleanly with `200 {messages: [], up_to_date: true,
        shutdown: true}` instead of touching the closed database.
        """
        with self._changed:
            self._stopped = True
            self._changed.notify_all()
        self.db.close()

    def compact(self, table: str, keep_last: int = 0) -> dict[str, int]:
        with self._changed:
            row = self.db.execute(
                "SELECT seq FROM _electrolite_log WHERE table_name = ? ORDER BY seq DESC LIMIT 1 OFFSET ?",
                (table, max(0, int(keep_last))),
            ).fetchone()
            retained_offset = int(row["seq"]) if row else self.high_water_mark()
            cursor = self.db.execute(
                "DELETE FROM _electrolite_log WHERE table_name = ? AND seq <= ?",
                (table, retained_offset),
            )
            self.db.execute(
                """
                INSERT INTO _electrolite_meta (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (f"retained_offset:{table}", str(retained_offset)),
            )
            self.db.commit()
            return {"retained_offset": retained_offset, "deleted_rows": cursor.rowcount}

    def execute(self, sql: str, params: Union[list[Any], tuple[Any, ...]] = ()) -> int:
        with self._changed:
            cursor = self.db.execute(sql, tuple(params))
            self.db.commit()
            self._changed.notify_all()
            return cursor.rowcount

    def execute_batch(self, sql: str) -> None:
        with self._changed:
            self.db.executescript(sql)
            self.db.commit()
            self._changed.notify_all()

    def write_batch(self, statements: list[tuple[str, Union[list[Any], tuple[Any, ...]]]]) -> None:
        batch_id = secrets.token_hex(16)
        with self._changed:
            try:
                self.db.execute("BEGIN IMMEDIATE")
                self.db.execute(
                    """
                    INSERT INTO _electrolite_meta (key, value)
                    VALUES ('current_batch_id', ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (batch_id,),
                )
                for sql, params in statements:
                    self.db.execute(sql, tuple(params))
                self.db.execute("DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'")
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            self._changed.notify_all()

    def handle(
        self,
        path: str,
        query: Optional[Union[str, dict[str, Any]]] = None,
        *,
        context: Any = None,
    ) -> tuple[int, dict[str, Any]]:
        try:
            route = self._parse_route(path, query)
        except BadInput as e:
            return 400, {"error": "bad_request", "detail": str(e)}
        if route is None:
            return 404, {"error": "shape_not_found"}
        definition = self.shapes.get(route["name"])
        if definition is None:
            return 404, {"error": "shape_not_found"}
        built = self._build_shape(definition, route, context)
        if built is None:
            return 404, {"error": "shape_not_found"}

        try:
            if route["offset"] >= 0 and route.get("log_id") and route["log_id"] != self.log_id():
                return 409, {"error": "resync_required"}
            if route["offset"] < 0:
                return 200, self.snapshot(built)
            body = self.replay(built, route["offset"], self.replay_limit)
            if route.get("shape_handle") and route["shape_handle"] != body["shape_handle"]:
                return 409, {"error": "resync_required"}
            if route["live"] and not body["messages"] and body["up_to_date"]:
                deadline = time.monotonic() + self.live_timeout_ms / 1000
                with self._changed:
                    remaining = deadline - time.monotonic()
                    if remaining > 0:
                        self._changed.wait(remaining)
                if self._stopped:
                    return 200, {
                        "type": "replay",
                        "log_id": body["log_id"],
                        "shape_handle": body["shape_handle"],
                        "messages": [],
                        "offset": route["offset"],
                        "up_to_date": True,
                        "shutdown": True,
                    }
                body = self.replay(built, route["offset"], self.replay_limit)
            return 200, body
        except ResyncRequired:
            return 409, {"error": "resync_required"}
        except BadInput as e:
            return 400, {"error": "bad_request", "detail": str(e)}

    def snapshot(self, shape_spec: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            info = self._validated_watched_table(shape_spec)
            normalized = self._normalize_shape(info, shape_spec)
            where, params = self._compile_predicate(info, normalized["predicate"])
            offset = self.high_water_mark()
            sql = (
                f"SELECT {self._row_json_expr('', shape_spec['columns'])} AS row_json "
                f"FROM {quote_ident(shape_spec['table'])}"
            )
            if where:
                sql += f" WHERE {where}"
            sql += " ORDER BY " + ", ".join(quote_ident(column) for column in info["pk_columns"])
            rows = [json.loads(row["row_json"]) for row in self.db.execute(sql, params)]
            return {
                "type": "snapshot",
                "log_id": self.log_id(),
                "shape_handle": shape_handle(normalized),
                "key_columns": info["pk_columns"],
                "rows": rows,
                "offset": offset,
                "up_to_date": True,
            }

    def replay(self, shape_spec: dict[str, Any], offset: int, limit: int = 1000) -> dict[str, Any]:
        with self._lock:
            info = self._validated_watched_table(shape_spec)
            normalized = self._normalize_shape(info, shape_spec)
            retained_offset = self._retained_offset(shape_spec["table"])
            if offset < retained_offset:
                raise ResyncRequired()
            page = self._read_log_page(shape_spec["table"], offset, max(1, int(limit)))
            messages: list[dict[str, Any]] = []
            latest = offset
            for row in page["rows"]:
                latest = max(latest, row["seq"])
                messages.extend(messages_for_log(normalized, row))
            return {
                "type": "replay",
                "log_id": self.log_id(),
                "shape_handle": shape_handle(normalized),
                "messages": messages,
                "offset": latest,
                "up_to_date": page["up_to_date"],
            }

    def high_water_mark(self) -> int:
        row = self.db.execute("SELECT COALESCE(MAX(seq), 0) AS seq FROM _electrolite_log").fetchone()
        return int(row["seq"])

    def log_id(self) -> str:
        row = self.db.execute("SELECT value FROM _electrolite_meta WHERE key = 'log_id'").fetchone()
        return str(row["value"])

    def bootstrap(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS _electrolite_meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS _electrolite_watched_tables (
              table_name TEXT PRIMARY KEY,
              pk_columns TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS _electrolite_log (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id TEXT NOT NULL,
              table_name TEXT NOT NULL,
              op TEXT NOT NULL,
              pk_json TEXT NOT NULL,
              old_pk_json TEXT,
              new_pk_json TEXT,
              old_json TEXT,
              new_json TEXT,
              created_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            CREATE INDEX IF NOT EXISTS _electrolite_log_table_seq_idx
              ON _electrolite_log (table_name, seq);
            """
        )
        self.db.execute(
            """
            INSERT INTO _electrolite_meta (key, value)
            SELECT 'log_id', ?
            WHERE NOT EXISTS (SELECT 1 FROM _electrolite_meta WHERE key = 'log_id')
            """,
            (secrets.token_hex(16),),
        )
        # A crashed write_batch may have left current_batch_id behind.
        # If we don't clear it, the next unrelated write would be tagged
        # as part of the dead batch.
        self.db.execute("DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'")
        self.db.commit()

    def _install_trigger_sql(self, info: dict[str, Any]) -> None:
        table = info["table"]
        old_row = self._row_json_expr("OLD", info["columns"])
        new_row = self._row_json_expr("NEW", info["columns"])
        old_pk = self._row_json_expr("OLD", info["pk_columns"])
        new_pk = self._row_json_expr("NEW", info["pk_columns"])
        batch_id = (
            "COALESCE((SELECT value FROM _electrolite_meta "
            "WHERE key = 'current_batch_id'), lower(hex(randomblob(16))))"
        )
        table_literal = quote_string(table)
        table_name = quote_ident(table)
        self.db.executescript(
            f"""
            DROP TRIGGER IF EXISTS {quote_ident(f'_electrolite_{table}_ai')};
            DROP TRIGGER IF EXISTS {quote_ident(f'_electrolite_{table}_au')};
            DROP TRIGGER IF EXISTS {quote_ident(f'_electrolite_{table}_ad')};

            CREATE TRIGGER {quote_ident(f'_electrolite_{table}_ai')}
            AFTER INSERT ON {table_name}
            BEGIN
              INSERT INTO _electrolite_log (
                batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
              )
              VALUES ({batch_id}, {table_literal}, 'insert', {new_pk}, NULL, {new_pk}, NULL, {new_row});
            END;

            CREATE TRIGGER {quote_ident(f'_electrolite_{table}_au')}
            AFTER UPDATE ON {table_name}
            BEGIN
              INSERT INTO _electrolite_log (
                batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
              )
              VALUES ({batch_id}, {table_literal}, 'update', {new_pk}, {old_pk}, {new_pk}, {old_row}, {new_row});
            END;

            CREATE TRIGGER {quote_ident(f'_electrolite_{table}_ad')}
            AFTER DELETE ON {table_name}
            BEGIN
              INSERT INTO _electrolite_log (
                batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
              )
              VALUES ({batch_id}, {table_literal}, 'delete', {old_pk}, {old_pk}, NULL, {old_row}, NULL);
            END;
            """
        )

    def _inspect_table(self, table: str) -> dict[str, Any]:
        rows = self.db.execute(f"PRAGMA table_info({quote_string(table)})").fetchall()
        if not rows:
            raise ValueError(f"table {table} does not exist")
        ordered = sorted(rows, key=lambda row: row["cid"])
        pk_rows = sorted((row for row in ordered if row["pk"] > 0), key=lambda row: row["pk"])
        return {
            "table": table,
            "columns": [row["name"] for row in ordered],
            "pk_columns": [row["name"] for row in pk_rows],
            "column_types": {row["name"]: row["type"] or "" for row in ordered},
        }

    def _validated_watched_table(self, shape_spec: dict[str, Any]) -> dict[str, Any]:
        info = self._inspect_table(shape_spec["table"])
        watched = self.db.execute(
            "SELECT pk_columns FROM _electrolite_watched_tables WHERE table_name = ?",
            (shape_spec["table"],),
        ).fetchone()
        if watched is None:
            raise ValueError(f"table {shape_spec['table']} is not watched by Electrolite")
        info["pk_columns"] = json.loads(watched["pk_columns"])
        for column in shape_spec["columns"]:
            if column not in info["columns"]:
                raise ValueError(f"shape column {column} does not exist")
        for column in info["pk_columns"]:
            if column not in shape_spec["columns"]:
                raise ValueError(f"shape columns must include primary key column {column}")
        return info

    def _row_json_expr(self, prefix: str, columns: list[str]) -> str:
        qualifier = f"{prefix}." if prefix else ""
        parts = [f"{quote_string(column)}, {qualifier}{quote_ident(column)}" for column in columns]
        return f"json_object({', '.join(parts)})"

    def _compile_predicate(self, info: dict[str, Any], predicate: Optional[Predicate]) -> tuple[str, list[Any]]:
        if predicate is None or predicate.get("type") == "all":
            return "", []
        if predicate["type"] == "eq":
            value = self._normalize_predicate_value(info, predicate["column"], predicate["value"])
            if value is None:
                return f"{quote_ident(predicate['column'])} IS NULL", []
            return f"{quote_ident(predicate['column'])} = ?", [value]
        if predicate["type"] in _RANGE_OPS:
            value = self._normalize_predicate_value(info, predicate["column"], predicate["value"])
            if value is None:
                raise BadInput(f"range predicate {predicate['type']} requires a non-null value")
            return f"{quote_ident(predicate['column'])} {_RANGE_OPS[predicate['type']]} ?", [value]
        if predicate["type"] == "in":
            values = [self._normalize_predicate_value(info, predicate["column"], value) for value in predicate["values"]]
            non_null = [value for value in values if value is not None]
            parts = []
            if not values:
                return "0", []
            if non_null:
                parts.append(f"{quote_ident(predicate['column'])} IN ({', '.join('?' for _ in non_null)})")
            if len(non_null) != len(values):
                parts.append(f"{quote_ident(predicate['column'])} IS NULL")
            return " OR ".join(f"({part})" for part in parts), non_null
        if predicate["type"] == "and":
            children = [self._compile_predicate(info, child) for child in predicate["predicates"]]
            children = [child for child in children if child[0]]
            return " AND ".join(f"({child[0]})" for child in children), [
                param for child in children for param in child[1]
            ]
        if predicate["type"] == "or":
            children = [self._compile_predicate(info, child) for child in predicate["predicates"]]
            children = [child for child in children if child[0]]
            if not children:
                return "", []
            return " OR ".join(f"({child[0]})" for child in children), [
                param for child in children for param in child[1]
            ]
        if predicate["type"] == "not":
            inner = predicate["predicate"]
            # Special case: not(eq(col, null)) → "col IS NOT NULL"
            # so SQLite can use the column index.
            if inner.get("type") == "eq" and inner.get("value") is None:
                column = quote_ident(inner["column"])
                return f"{column} IS NOT NULL", []
            child_sql, child_args = self._compile_predicate(info, inner)
            if not child_sql:
                return "0", []
            return f"NOT ({child_sql})", child_args
        raise ValueError(f"unsupported predicate type {predicate.get('type')}")

    def _normalize_shape(self, info: dict[str, Any], shape_spec: dict[str, Any]) -> dict[str, Any]:
        return {
            "table": shape_spec["table"],
            "columns": sorted(shape_spec["columns"]),
            "predicate": self._normalize_predicate(info, shape_spec.get("predicate") or all()),
            "auth_scope": shape_spec.get("auth_scope", ""),
            "schema_version": shape_spec.get("schema_version", 1),
        }

    def _normalize_predicate(self, info: dict[str, Any], predicate: Optional[Predicate]) -> Predicate:
        if predicate is None or predicate.get("type") == "all":
            return all()
        if predicate["type"] == "eq":
            return {
                "type": "eq",
                "column": predicate["column"],
                "value": self._normalize_predicate_value(info, predicate["column"], predicate["value"]),
            }
        if predicate["type"] in _RANGE_OPS:
            return {
                "type": predicate["type"],
                "column": predicate["column"],
                "value": self._normalize_predicate_value(info, predicate["column"], predicate["value"]),
            }
        if predicate["type"] == "in":
            values = [
                self._normalize_predicate_value(info, predicate["column"], value)
                for value in predicate["values"]
            ]
            deduped = sorted({json.dumps(value, sort_keys=True) for value in values})
            return {"type": "in", "column": predicate["column"], "values": [json.loads(value) for value in deduped]}
        if predicate["type"] == "and":
            children = [self._normalize_predicate(info, child) for child in predicate["predicates"]]
            children.sort(key=lambda child: json.dumps(child, sort_keys=True))
            return {"type": "and", "predicates": children}
        if predicate["type"] == "or":
            children = [self._normalize_predicate(info, child) for child in predicate["predicates"]]
            children.sort(key=lambda child: json.dumps(child, sort_keys=True))
            return {"type": "or", "predicates": children}
        if predicate["type"] == "not":
            return {
                "type": "not",
                "predicate": self._normalize_predicate(info, predicate["predicate"]),
            }
        raise ValueError(f"unsupported predicate type {predicate.get('type')}")

    def _normalize_predicate_value(self, info: dict[str, Any], column: str, value: Any) -> Any:
        if column not in info["columns"]:
            raise BadInput(f"predicate column {column} does not exist")
        if value is None:
            return None
        column_type = str(info["column_types"].get(column, "")).upper()
        booleanish = "BOOL" in column_type
        if isinstance(value, bool):
            if booleanish:
                return 1 if value else 0
            raise BadInput("boolean predicates require BOOLEAN columns")
        return value

    def _read_log_page(self, table: str, offset: int, limit: int) -> dict[str, Any]:
        rows = [
            parse_log_row(row)
            for row in self.db.execute(
                """
                SELECT seq, batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json, created_at
                FROM _electrolite_log
                WHERE table_name = ? AND seq > ?
                ORDER BY seq
                LIMIT ?
                """,
                (table, offset, limit),
            )
        ]
        if rows:
            last = rows[-1]
            # Extend until the trailing batch finishes or until we hit
            # the safety cap. Without the cap, a 10M-row batch would
            # force replay to load every row in one response.
            extension_cap = max(limit, 1) * 10
            rows.extend(
                parse_log_row(row)
                for row in self.db.execute(
                    """
                    SELECT seq, batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json, created_at
                    FROM _electrolite_log
                    WHERE table_name = ? AND seq > ? AND batch_id = ?
                    ORDER BY seq
                    LIMIT ?
                    """,
                    (table, last["seq"], last["batch_id"], extension_cap),
                )
            )
        latest = rows[-1]["seq"] if rows else offset
        newer = self.db.execute(
            "SELECT 1 FROM _electrolite_log WHERE table_name = ? AND seq > ? LIMIT 1",
            (table, latest),
        ).fetchone()
        return {"rows": rows, "up_to_date": newer is None}

    def _retained_offset(self, table: str) -> int:
        row = self.db.execute(
            "SELECT value FROM _electrolite_meta WHERE key = ?",
            (f"retained_offset:{table}",),
        ).fetchone()
        return int(row["value"]) if row else 0

    def _parse_route(self, path: str, query: Optional[Union[str, dict[str, Any]]]) -> Optional[dict[str, Any]]:
        if not path.startswith(self.prefix + "/"):
            return None
        rest = path[len(self.prefix) + 1 :]
        parts = [part for part in rest.split("/") if part]
        if not parts:
            return None
        query_dict = parse_query(query)
        offset_raw = first(query_dict.get("offset"), "-1")
        try:
            offset = int(offset_raw)
        except (TypeError, ValueError):
            raise BadInput(f"offset must be an integer, got {offset_raw!r}")
        return {
            "name": parts[0],
            "params": parts[1:],
            "offset": offset,
            "live": first(query_dict.get("live"), "false") == "true",
            "log_id": first(query_dict.get("log_id")),
            "shape_handle": first(query_dict.get("shape_handle")),
        }

    def _build_shape(self, definition: Shape, route: dict[str, Any], context: Any) -> Optional[dict[str, Any]]:
        param_names = definition.params or []
        params = dict(zip(param_names, route["params"]))
        build_context = {"params": params, "context": context}
        auth_scope = definition.scope(build_context) if callable(definition.scope) else definition.scope
        auth_scope = auth_scope or ""
        if definition.authorize and not definition.authorize({**build_context, "scope": auth_scope}):
            return None
        predicate = definition.where(build_context) if definition.where else all()
        return {
            "name": route["name"],
            "table": definition.table,
            "columns": definition.columns,
            "predicate": predicate,
            "auth_scope": auth_scope,
            "schema_version": definition.schema_version,
        }


class ResyncRequired(Exception):
    pass


class BadInput(ValueError):
    """Predicate or argument validation failure → 400."""
    pass


def create_electrolite(*args: Any, **kwargs: Any) -> Electrolite:
    return Electrolite(*args, **kwargs)


def parse_log_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "seq": int(row["seq"]),
        "batch_id": row["batch_id"],
        "table_name": row["table_name"],
        "op": row["op"],
        "pk_json": json.loads(row["pk_json"]),
        "old_pk_json": json.loads(row["old_pk_json"]) if row["old_pk_json"] else None,
        "new_pk_json": json.loads(row["new_pk_json"]) if row["new_pk_json"] else None,
        "old_json": json.loads(row["old_json"]) if row["old_json"] else None,
        "new_json": json.loads(row["new_json"]) if row["new_json"] else None,
        "created_at": int(row["created_at"]),
    }


def messages_for_log(shape_spec: dict[str, Any], row: dict[str, Any]) -> list[dict[str, Any]]:
    old_matches = predicate_matches(shape_spec["predicate"], row["old_json"])
    new_matches = predicate_matches(shape_spec["predicate"], row["new_json"])
    old_key = row["old_pk_json"] or row["pk_json"]
    new_key = row["new_pk_json"] or row["pk_json"]
    if not old_matches and new_matches and row["new_json"]:
        return [message("insert", row, new_key, row["new_json"])]
    if old_matches and new_matches and row["new_json"]:
        if old_key == new_key:
            return [message("update", row, new_key, row["new_json"])]
        return [message("delete", row, old_key), message("insert", row, new_key, row["new_json"])]
    if old_matches and not new_matches:
        return [message("delete", row, old_key)]
    return []


def message(kind: str, row: dict[str, Any], key: dict[str, Any], value: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    out = {"type": kind, "batch_id": row["batch_id"], "key": key, "offset": row["seq"]}
    if value is not None:
        out["value"] = value
    return out


def predicate_matches(predicate: Optional[Predicate], row: Optional[dict[str, Any]]) -> bool:
    if row is None:
        return False
    if predicate is None or predicate.get("type") == "all":
        return True
    if predicate["type"] == "eq":
        return row.get(predicate["column"]) == predicate["value"]
    if predicate["type"] in _RANGE_OPS:
        return _compare_scalar(row.get(predicate["column"]), predicate["value"], predicate["type"])
    if predicate["type"] == "in":
        return row.get(predicate["column"]) in predicate["values"]
    if predicate["type"] == "and":
        return builtins.all(predicate_matches(child, row) for child in predicate["predicates"])
    if predicate["type"] == "or":
        return any(predicate_matches(child, row) for child in predicate["predicates"])
    if predicate["type"] == "not":
        return not predicate_matches(predicate["predicate"], row)
    raise ValueError(f"unsupported predicate type {predicate.get('type')}")


def _compare_scalar(left: Any, right: Any, op: str) -> bool:
    if left is None or right is None:
        return False
    if type(left) is bool or type(right) is bool:
        return False
    if not isinstance(left, type(right)) and not isinstance(right, type(left)):
        return False
    if op == "gt":
        return left > right
    if op == "lt":
        return left < right
    if op == "gte":
        return left >= right
    if op == "lte":
        return left <= right
    return False


def shape_handle(shape_spec: dict[str, Any]) -> str:
    body = json.dumps(shape_spec, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def parse_query(query: Optional[Union[str, dict[str, Any]]]) -> dict[str, Any]:
    if query is None:
        return {}
    if isinstance(query, str):
        return parse_qs(query.lstrip("?"))
    return query


def first(value: Any, default: Any = None) -> Any:
    if isinstance(value, list):
        return value[0] if value else default
    return value if value is not None else default


def quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def quote_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"
