"""Shape definition helpers.

This is intentionally tiny for now. It gives the design docs a concrete
object to point at without pretending the sync engine exists yet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Any


@dataclass(frozen=True)
class Shape:
    name: str
    table: str
    where: dict[str, Any]
    columns: list[str]
    auth_scope: str = "public"
    schema_version: int = 1
    handle: str = field(init=False)

    def __post_init__(self) -> None:
        payload = {
            "table": self.table,
            "where": self.where,
            "columns": self.columns,
            "auth_scope": self.auth_scope,
            "schema_version": self.schema_version,
        }
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        object.__setattr__(self, "handle", sha256(text.encode("utf-8")).hexdigest())
