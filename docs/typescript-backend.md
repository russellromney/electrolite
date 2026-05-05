# Legacy TypeScript Backend Bridge

New TypeScript apps should use the native package in
[node-native.md](node-native.md). It embeds the Rust core directly in
Node/Bun through Node-API, so there is no separate Electrolite origin to
run.

This document describes the older bridge for apps that want a
TypeScript authorization layer in front of an internal Electrolite HTTP
origin. It remains useful for experiments and for comparing deployment
models, but it is no longer the preferred TypeScript integration path.

The core target is embedded: the host application owns SQLite, writes,
authorization, and the Electrolite endpoint. In the bridge model, the
internal Electrolite origin has the trusted Shape factory enabled, and a
TypeScript proxy is mounted in your app. That origin can be a loopback
Rust process. A separate sidecar is optional, not required by the
protocol.

The browser talks to the TypeScript app. The TypeScript app checks the
user's session/RBAC and forwards only authorized Shape requests to the
Electrolite origin.

```text
browser
  -> TypeScript app route
  -> app checks session/RBAC
  -> app forwards scoped request to internal Electrolite origin
  -> SQLite
```

The helper in `clients/typescript-backend` uses the Web Fetch API, so it
fits Node, Bun, Hono, Next route handlers, and other Request/Response
based runtimes.

## Example

Internal Electrolite origin:

```rust
use electrolite_core::ShapeRegistry;
use electrolite_server::{
    ServerState,
    TrustedHeaderAuthorizer,
    TrustedHeaderShapeFactory,
    TRUSTED_SHAPE_FACTORY_NAME,
};

let state = ServerState::new(db_path, ShapeRegistry::new(), TrustedHeaderAuthorizer)
    .with_shape_factory(TRUSTED_SHAPE_FACTORY_NAME, TrustedHeaderShapeFactory);
```

TypeScript app route:

```ts
import {
  createElectroliteProxy,
  trustedShapeHeaders,
} from "@electrolite/backend";

const electrolite = createElectroliteProxy({
  origin: "http://127.0.0.1:5137",
  authorize: async ({ request, kind, name, path }) => {
    const session = await getSession(request);
    if (!session) {
      return false;
    }

    if (kind === "factory" && name === "trusted") {
      const [shapeName, projectId] = path.split("/");
      if (shapeName !== "projectTodos" || !projectId) {
        return false;
      }
      if (!(await canReadProject(session.userId, projectId))) {
        return false;
      }

      return {
        allow: true,
        headers: {
          ...trustedShapeHeaders({
            name: `projectTodos/${projectId}`,
            table: "todos",
            columns: ["id", "project_id", "title", "done"],
            predicate: {
              type: "eq",
              column: "project_id",
              value: projectId,
            },
            auth_scope: `project:${projectId}`,
            schema_version: 1,
          }),
          "x-electrolite-scope": `project:${projectId}`,
        },
      };
    }

    return false;
  },
});

export async function GET(request: Request) {
  return electrolite(request);
}
```

The Electrolite origin owns SQLite reads. The TypeScript app owns who may
ask for which Shape and can construct the concrete Shape spec from app
route params. By default, the proxy forwards only safe cache/request
headers plus the headers returned by `authorize`; browser cookies and
bearer tokens are not forwarded to the Electrolite origin.

## Routes

Static Shapes:

```http
GET /electrolite/v1/shape/:name?offset=-1
```

Dynamic factory Shapes:

```http
GET /electrolite/v1/factory/:factory/:path?offset=-1
```

Denied requests return the same public body as missing Shapes:

```json
{ "error": "shape_not_found" }
```
