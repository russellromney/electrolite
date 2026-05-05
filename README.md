# electrolite

Embeddable Electric-style shape sync for SQLite.

Electrolite is a Rust-first project for syncing named SQLite table shapes
to browsers without a separate sync daemon. The intended shape:

```text
SQLite + generated triggers
  -> durable logical change log
  -> app-embedded HTTP shape endpoint
  -> browser client consumes snapshot + offset log
```

The semantic core is a trigger-backed logical log. Honker-style commit
wakes, Walrust physical replication, and S3/Cinch object storage are
useful accelerants, but not required for the first version.

## Workspace

- `crates/electrolite-core` - shape definitions, handles, log rows, and
  membership transition logic.
- `crates/electrolite-sqlite` - SQLite metadata tables, trigger
  generation, and log reads.
- `crates/electrolite-server` - future embedded HTTP long-poll service.

## Goals

- Electric-like initial snapshot plus live offset replay for SQLite.
- Named server-side shapes instead of arbitrary browser SQL.
- Browser delivery over cache-friendly HTTP long-polling.
- Strong security defaults: app-authorized shapes, column allowlists,
  private raw logs, and short-lived signed shape URLs when needed.
- Honest fanout economics: excellent for shared team/workspace/document
  shapes, explicit tradeoffs for per-user private shapes.

## Non-goals

- Postgres replication.
- Arbitrary client-provided SQL.
- Offline writes or conflict resolution in the first version.
- A required standalone sync daemon.

## Roadmap

See [ROADMAP.md](ROADMAP.md).

## Status

Early scaffold. The first implemented slice is trigger-backed logical
change capture for simple primary-key tables.
