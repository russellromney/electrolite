# electrolite

Embeddable Electric-style sync for SQLite.

Electrolite is a Rust-first experiment inspired directly by
[ElectricSQL](https://electric-sql.com/) and its
[Electric Sync](https://electric.ax/docs/sync/) engine. Electric Sync is
a Postgres read-path sync engine: it consumes Postgres logical
replication, exposes selected subsets of database rows over HTTP, and
lets clients materialize those subsets with an initial sync followed by
live logical updates.

Electrolite tries to preserve that lifecycle for SQLite without requiring
a separate sync daemon. The intended architecture:

```text
SQLite + generated triggers
  -> durable logical change log
  -> app-embedded HTTP sync endpoint
  -> browser client consumes snapshot + offset log
```

The semantic core is a trigger-backed logical log. Honker-style commit
wakes, Walrust physical replication, and S3/Cinch object storage are
useful accelerants, but not required for the first version.

## The Sync Primitive

ElectricSQL calls one of these synced subsets a Shape. A Shape is a
client-consumable subset of a database, delivered as an HTTP log that
starts with current rows and then continues with inserts, updates, and
deletes.

In Electrolite today, a Shape is server-defined and contains:

- a source table
- a column allowlist
- a predicate, currently simple equality and conjunctions
- an authorization scope
- a schema version

Browsers do not send arbitrary SQL. They request named Shapes that the
host application has already defined and authorized.

## Workspace

- `crates/electrolite-core` - Shape definitions, handles, log rows, and
  membership transition logic.
- `crates/electrolite-sqlite` - SQLite metadata tables, trigger
  generation, and log reads.
- `crates/electrolite-server` - embedded HTTP snapshot and replay routes.

## Goals

- Electric-like initial snapshot plus live offset replay for SQLite.
- Named server-side Shapes instead of arbitrary browser SQL.
- Browser delivery over cache-friendly HTTP long-polling.
- Strong security defaults: app-authorized Shapes, column allowlists,
  private raw logs, and short-lived signed Shape URLs when needed.
- Honest fanout economics: excellent for shared team/workspace/document
  Shapes, explicit tradeoffs for per-user private Shapes.

## Non-goals

- Postgres replication.
- Arbitrary client-provided SQL.
- Offline writes or conflict resolution in the first version.
- A required standalone sync daemon.

## Roadmap

See [ROADMAP.md](ROADMAP.md).

## Status

Early scaffold. The implemented slice is trigger-backed logical change
capture for simple primary-key tables, plus an embedded HTTP route for
initial snapshots and bounded replay.
