# Electrolite Node API

TypeScript-first Electric-style sync for SQLite.

You write TypeScript. Electrolite installs SQLite triggers, keeps a
durable change log, and serves browser Shapes over normal HTTP.

Electrolite is TypeScript on the server side and uses Node's built-in
SQLite API. You need Node 24+.

```ts
import {
  createElectrolite,
  eq,
  shape,
} from "./vendor/electrolite/packages/electrolite-node/electrolite-node.ts";

const electrolite = createElectrolite({
  dbPath: "./app.db",
  shapes: {
    projectTodos: shape({
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      params: ["projectId"],
      where: ({ params }) => eq("project_id", params.projectId),
    }),
  },
});

electrolite.installTriggers("todos");

export default {
  fetch(request: Request) {
    return electrolite.handle(request);
  },
};
```

Browser clients can then subscribe to:

```text
/electrolite/v1/projectTodos/p1
```

## Shared caching (`cacheable`)

Snapshot and replay responses are `cache-control: private` by default,
so a CDN or shared proxy will not serve one user's authorized Shape to
another user. Live responses are always `no-store`.

A Shape that is safe for any holder of its URL can opt into `public`
(CDN-shareable) caching:

```ts
publicChangelog: shape({
  table: "changelog",
  columns: ["id", "title", "published_at"],
  where: () => eq("published", 1),
  cacheable: true, // public, max-age, immutable on replay pages
}),
```

Only set `cacheable: true` when the Shape carries no per-user data, OR
you deliver it via short-lived signed URLs, OR auth is in the
`Authorization` header (covered by `vary: authorization`). Cookie or
query-token auth is **not** safe to mark cacheable.

## Recommended PRAGMAs

The engine does not issue `PRAGMA` statements; the user owns those.
For production-shaped apps:

```ts
import { DatabaseSync } from "node:sqlite";
const db = new DatabaseSync("./app.db");
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
db.exec("PRAGMA busy_timeout = 5000");
db.close();
```

Set these once before constructing the Electrolite instance against
the same file.

## Use Before npm

Electrolite is not published to npm yet. Use this repository directly,
or vendor it into your app and import by path.

There is no native build step:

```sh
npm test --prefix packages/electrolite-node
```
