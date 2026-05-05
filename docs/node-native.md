# TypeScript Backend API

`@electrolite/node` is the main Electrolite API. It is designed for
TypeScript backends first: a Node or Bun app owns SQLite writes,
authorization, and the sync HTTP route without running a separate
Electrolite server.

The package is fast because the SQLite sync engine, trigger installer,
snapshot reader, and replay logic run in Rust underneath through a
native Node-API module. TypeScript code defines Shapes and authorization;
Rust does the tight database work.

```text
browser ShapeClient
  -> your TypeScript route
  -> @electrolite/node authorizes and serves the Shape
  -> SQLite + Electrolite triggers
```

The package is currently local to this repository at
`packages/electrolite-node`.

## Basic Use

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
  if (!session) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }

  return electrolite.handle(request, {
    user: { projects: await projectsForUser(session.userId) },
  });
}
```

Clients then request a server-owned Shape instance:

```ts
import { ShapeClient } from "./electrolite-browser-client.js";

const todos = new ShapeClient(
  "/electrolite/v1/projectTodos/project-123",
);

await todos.request({ offset: -1 });
```

## Writes

Writes should go through the same Electrolite instance when possible so
live long-poll requests wake immediately:

```ts
electrolite.execute(
  "INSERT INTO todos (project_id, title) VALUES (?1, ?2)",
  ["project-123", "Ship native binding"],
);
```

For app-controlled multi-statement writes, use `writeBatch`. Electrolite
records the rows with one batch marker so bounded replay does not split
that app-level change across responses.

```ts
electrolite.writeBatch([
  [
    "INSERT INTO todos (project_id, title) VALUES (?1, ?2)",
    ["project-123", "first"],
  ],
  [
    "INSERT INTO todos (project_id, title) VALUES (?1, ?2)",
    ["project-123", "second"],
  ],
]);
```

## Route Contract

The native package serves:

```http
GET /electrolite/v1/:shapeName/:param...?offset=-1
GET /electrolite/v1/:shapeName/:param...?offset=123
GET /electrolite/v1/:shapeName/:param...?offset=123&live=true
```

Denied and missing Shapes both return `404 shape_not_found`, so Shape
names are not exposed as an authorization side channel. If retained log
history no longer covers a requested offset, the route returns
`409 resync_required` and the browser client should request a fresh
snapshot.

## Current Limits

- Predicates are intentionally Shape-oriented, not arbitrary SQL:
  `all`, equality, `IN`, and conjunction.
- The native package opens SQLite connections per call today. The Rust
  embedded server already has pooling; the Node package should grow a
  small connection pool next.
- IndexedDB persistence, React hooks, and multi-tab coordination belong
  in the browser client and are not implemented yet.
