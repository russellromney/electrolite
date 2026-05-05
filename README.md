# electrolite

Fast Electric-style sync for SQLite, exposed as a TypeScript package.

Electrolite is a TypeScript-first embedded sync library inspired directly by
[ElectricSQL](https://electric-sql.com/) and its
[Electric Sync](https://electric.ax/docs/sync/) engine. Electric Sync is
a Postgres read-path sync engine: it consumes Postgres logical
replication, exposes selected subsets of database rows called Shapes over
HTTP, and lets clients materialize those Shapes with an initial sync
followed by live logical updates.

Electrolite tries to preserve that lifecycle for SQLite without requiring
a separate sync daemon. Apps use the `@electrolite/node` API from
Node/Bun, while the hot path runs in Rust underneath through a native
Node-API binding.

The intended architecture:

```text
SQLite + generated triggers
  -> durable logical change log
  -> TypeScript app-embedded HTTP sync endpoint
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
- a predicate, currently equality, `IN`, and conjunctions
- an authorization scope
- a schema version

Browsers do not send arbitrary SQL. They request named Shapes that the
host application has already defined and authorized.

Applications can also register dynamic, server-owned Shape routes such
as `/projects/:project_id/todos`. The route turns request path/auth
context into a concrete Shape, and the normal authorizer still checks
the generated authorization scope before SQLite is touched. TypeScript
app servers define those routes directly with `@electrolite/node`, so
browsers get Electric-style sync without being allowed to send SQL.

## TypeScript Quick Start

```ts
import { createElectrolite, eq, shape } from "@electrolite/node";

const electrolite = createElectrolite<{ user: { projects: Set<string> } }>({
  dbPath: "./app.db",
  shapes: {
    projectTodos: shape({
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      params: ["projectId"],
      where: ({ params }) => eq("project_id", params.projectId),
      scope: ({ params }) => `project:${params.projectId}`,
      authorize: ({ params, context }) => {
        return context.user.projects.has(params.projectId);
      },
    }),
  },
});

electrolite.executeBatch(`
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL,
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL DEFAULT 0
  );
`);
electrolite.installTriggers("todos");

export async function GET(request: Request) {
  const session = await getSession(request);
  return electrolite.handle(request, {
    user: { projects: await projectsForUser(session.userId) },
  });
}
```

The browser requests `/electrolite/v1/projectTodos/project-123?offset=-1`
for the initial Shape snapshot, then continues from the returned offset
with ordinary replay or `live=true` long-polling.

## Workspace

- `crates/electrolite-core` - Shape definitions, handles, log rows, and
  membership transition logic.
- `crates/electrolite-sqlite` - SQLite metadata tables, trigger
  generation, and log reads.
- `crates/electrolite-server` - embedded authorized HTTP snapshot and
  replay routes.
- `packages/electrolite-node` - Node/Bun native binding that exposes the
  Rust core as a TypeScript-friendly embedded backend package.
- `clients/browser` - dependency-free browser materializer for Shape
  snapshots and live replay messages.
- `clients/typescript-backend` - dependency-free Web Fetch proxy helper
  for the older internal-origin bridge. New TypeScript apps should start
  with `packages/electrolite-node`.

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

For the user-facing TypeScript API, start with
[docs/node-native.md](docs/node-native.md). The older internal-origin
bridge is documented separately in
[docs/typescript-backend.md](docs/typescript-backend.md).

## Status

Early scaffold. The implemented slice is trigger-backed logical change
capture for primary-keyed SQLite tables, plus embedded HTTP routes for
authorized initial snapshots, bounded replay, and `live=true`
long-polling. Rust apps can use `electrolite-server` directly;
TypeScript apps can use `@electrolite/node`, which loads the Rust core
through a native Node-API binding and exposes a Web Fetch route handler.
The server has a SQLite connection pool, in-process live wait coalescing,
retained-offset resync errors, and a basic fanout benchmark harness.
Dynamic Shape factories and a table/equality predicate index are in place
for the next fanout broker layer. Embedded write helpers can wake live
requests automatically, retention compaction records a durable retained
offset, and optional Electrolite change batches avoid splitting
app-controlled transactions across bounded replay responses. Responses
include key-column metadata and an explicit `up_to_date` boundary; Shape
handles are canonicalized across equivalent definitions; and SQLite
predicate values are normalized against declared column types to avoid
snapshot/replay drift.
