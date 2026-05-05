from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Iterable


class ElectroliteError(Exception):
    pass


@dataclass
class HttpResponse:
    status: int
    body: Any = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def json(self) -> Any:
        return self.body


class ShapeClient:
    """Small synchronous Electrolite client for Python scripts and services."""

    def __init__(
        self,
        url: str,
        *,
        key_columns: Iterable[str] | None = None,
        fetch: Callable[[str], HttpResponse] | None = None,
        retry_min_delay: float = 0.25,
        retry_max_delay: float = 5.0,
    ) -> None:
        if key_columns is not None:
            key_columns = list(key_columns)
            if not key_columns:
                raise ValueError("key_columns must be non-empty when provided")

        self.url = url
        self.key_columns = list(key_columns) if key_columns is not None else None
        self.fetch = fetch or default_fetch
        self.retry_min_delay = retry_min_delay
        self.retry_max_delay = retry_max_delay
        self.offset = -1
        self.rows: dict[str, dict[str, Any]] = {}
        self.pending_rows: dict[str, dict[str, Any]] | None = None
        self.pending_changed = False
        self.subscribers: list[Callable[[list[dict[str, Any]]], None]] = []

    def current_rows(self) -> list[dict[str, Any]]:
        return list(self.rows.values())

    def subscribe(self, callback: Callable[[list[dict[str, Any]]], None]) -> Callable[[], None]:
        self.subscribers.append(callback)
        callback(self.current_rows())

        def unsubscribe() -> None:
            if callback in self.subscribers:
                self.subscribers.remove(callback)

        return unsubscribe

    def request(self, *, offset: int | None = None, live: bool = False) -> bool:
        response = self.fetch(self.request_url(offset=self.offset if offset is None else offset, live=live))
        if response.status == 204:
            return False
        if response.status == 409:
            self.offset = -1
            self.rows.clear()
            self.notify()
            return self.request(offset=-1)
        if not response.ok:
            raise ElectroliteError(f"Electrolite request failed: {response.status}")

        return self.apply(response.json())

    def start(self, *, live: bool = True, stop: Callable[[], bool] | None = None) -> None:
        delay = self.retry_min_delay
        stop = stop or (lambda: False)

        while not stop():
            try:
                if self.offset < 0:
                    self.request(offset=-1)
                elif not live:
                    return
                else:
                    self.request(offset=self.offset, live=True)
                delay = self.retry_min_delay
            except ElectroliteError:
                time.sleep(delay)
                delay = min(delay * 2, self.retry_max_delay)

    def request_url(self, *, offset: int, live: bool = False) -> str:
        parsed = urllib.parse.urlsplit(self.url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        query = [(key, value) for key, value in query if key not in {"offset", "live"}]
        query.append(("offset", str(offset)))
        if live:
            query.append(("live", "true"))
        return urllib.parse.urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urllib.parse.urlencode(query),
                parsed.fragment,
            )
        )

    def apply(self, body: dict[str, Any]) -> bool:
        message_type = body.get("type")
        if message_type == "snapshot":
            key_columns = body.get("key_columns")
            if isinstance(key_columns, list) and key_columns:
                self.key_columns = list(key_columns)
            self.require_key_columns()
            self.rows.clear()
            self.pending_rows = None
            self.pending_changed = False
            for row in body.get("rows", []):
                self.rows[self.key_for_row(row)] = row
            self.offset = body["offset"]
            self.notify()
            return True

        if message_type == "replay":
            changed = False
            next_rows = dict(self.pending_rows if self.pending_rows is not None else self.rows)
            for message in body.get("messages", []):
                changed = self.apply_message_to(next_rows, message) or changed

            self.offset = body["offset"]
            if body.get("up_to_date") is False:
                self.pending_rows = next_rows
                self.pending_changed = self.pending_changed or changed
                return False

            self.rows = next_rows
            changed = self.pending_changed or changed
            self.pending_rows = None
            self.pending_changed = False
            if changed:
                self.notify()
            return changed

        raise ElectroliteError(f"unknown Electrolite response type: {message_type}")

    def apply_message_to(self, rows: dict[str, dict[str, Any]], message: dict[str, Any]) -> bool:
        key = stable_key(message["key"])
        message_type = message.get("type")
        if message_type == "delete":
            return rows.pop(key, None) is not None
        if message_type in {"insert", "update"}:
            rows[key] = message["value"]
            return True
        raise ElectroliteError(f"unknown Electrolite message type: {message_type}")

    def key_for_row(self, row: dict[str, Any]) -> str:
        self.require_key_columns()
        return stable_key({column: row.get(column) for column in self.key_columns or []})

    def require_key_columns(self) -> None:
        if not self.key_columns:
            raise ElectroliteError("snapshot must include key_columns or key_columns must be provided")

    def notify(self) -> None:
        rows = self.current_rows()
        for subscriber in list(self.subscribers):
            subscriber(rows)


def stable_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def default_fetch(url: str) -> HttpResponse:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request) as response:
            status = response.status
            if status == 204:
                return HttpResponse(status)
            body = response.read().decode("utf-8")
            return HttpResponse(status, json.loads(body) if body else None)
    except urllib.error.HTTPError as error:
        if error.code == 204:
            return HttpResponse(204)
        body = error.read().decode("utf-8")
        parsed = json.loads(body) if body else None
        return HttpResponse(error.code, parsed)
