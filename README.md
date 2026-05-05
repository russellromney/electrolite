# electrolite

Embeddable Electric-style sync for SQLite.

Electrolite is a Rust-first experiment inspired directly by
[ElectricSQL](https://electric-sql.com/) and its
[Electric Sync](https://electric.ax/docs/sync/) engine. Electric Sync is
a Postgres read-path sync engine: it consumes Postgres logical
replication, exposes selected subsets of database rows called Shapes over
HTTP, and lets clients materialize those Shapes with an initial sync
followed by live logical updates.

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

## Shape Definition

A Shape is a client-consumable subset of a database, delivered as an HTTP
log that starts with current rows and then continues with inserts,
updates, and deletes.

In Electrolite today, a Shape is server-defined and contains:

- a source table
- a column allowlist
- a predicate, currently simple equality and conjunctions
- an authorization scope
- a schema version

Browsers do not send arbitrary SQL. They request named Shapes that the
host application has already defined and authorized.

Applications can also register Shape factories for dynamic, server-owned
routes such as `/projects/:project_id/todos`. A factory turns request
path/auth context into a concrete Shape, and the normal authorizer still
checks the generated authorization scope before SQLite is touched.
TypeScript app servers can use the trusted-header factory plus the
backend proxy helper to authorize and construct those concrete Shapes
without letting browsers send SQL.

## Workspace

- `crates/electrolite-core` - Shape definitions, handles, log rows, and
  membership transition logic.
- `crates/electrolite-sqlite` - SQLite metadata tables, trigger
  generation, and log reads.
- `crates/electrolite-server` - embedded authorized HTTP snapshot and
  replay routes.
- `clients/browser` - dependency-free browser materializer for Shape
  snapshots and live replay messages.
- `clients/typescript-backend` - dependency-free Web Fetch proxy helper
  for TypeScript app servers that authorize requests before forwarding
  them to an internal Electrolite origin.

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

For TypeScript app-server integration, see
[docs/typescript-backend.md](docs/typescript-backend.md).

## Status

Early scaffold. The implemented slice is trigger-backed logical change
capture for simple primary-key tables, plus an embedded HTTP route for
authorized initial snapshots, bounded replay, and `live=true`
long-polling. The server now has a SQLite connection pool, in-process
live wait coalescing, retained-offset resync errors, and a basic fanout
benchmark harness. Dynamic Shape factories and a table/equality predicate
index are in place for the next fanout broker layer.
