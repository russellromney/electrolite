# Security Model

Electrolite should default to app-authorized named shapes.

## Default: App-Authorized Shape Proxy

```text
browser
  -> app server route
  -> app checks session/RBAC
  -> app serves Electrolite shape
```

The browser never sends arbitrary SQL. It asks for named shapes that the
app has already defined.

In the embedded server, the host app passes an explicit `Authorizer` when
constructing `ServerState`. The authorizer receives request headers,
request extensions, the named Shape, the requested offset, and whether the
request is a live long-poll.

```rust
let state = ServerState::new(db_path, registry, AppAuthorizer);
```

The route denies unauthorized requests before opening SQLite or reading
the Electrolite log. Denied Shapes return `404 Not Found` by default so
callers cannot distinguish private Shape names from missing Shape names.
`AllowAll` exists for tests and local demos, but it must be selected
explicitly.

Host apps should authenticate before the Electrolite route runs, then put
their session/user object into request extensions. The authorizer maps
that app-specific identity to `shape.auth_scope`.

## Signed Shape URLs

Signed shape URLs are future work. For CDN or object-store delivery, the
app can mint signed shape URLs. The signature covers:

- shape handle
- auth scope
- columns
- params
- schema version
- expiration
- retention generation

## Rules

- Raw `_electrolite_log` is private.
- Authorization happens before SQLite is opened.
- SQLite and Electrolite internals are not serialized into HTTP error
  bodies.
- Public/object-store data must be authorized shape output, not global
  database changes.
- Shape handles include auth scope.
- Column allowlists are required.
- Delete messages must not reveal rows that were never visible to that
  authorized shape.
- Signed URLs should be short-lived for sensitive data.
