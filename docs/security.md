# Security Model

Electrolite should default to app-authorized named shapes.

## Default: App-Authorized Embedded Route

```text
browser
  -> app server route
  -> app checks session/RBAC
  -> app serves Electrolite shape
```

The browser never sends arbitrary SQL. It asks for named shapes that the
app has already defined.

TypeScript app servers define Shapes with an `authorize` hook. The hook
receives the request, route params, and app context, then returns whether
the current user may see that Shape. Unauthorized requests are denied
before SQLite is read. Denied Shapes return the same public response as
missing Shapes, so callers cannot distinguish private Shape names from
missing Shape names.

Host apps should authenticate before the Electrolite route runs, then
pass the session/user object as the handler context. The Shape definition
maps that app-specific identity to an `auth_scope`, which is included in
the Shape handle.

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

## Caching

Snapshot and replay responses default to `cache-control: private`. A
`private` response may be stored by the requesting browser's own HTTP
cache but never by a shared cache or CDN. This matters because a shared
cache keyed on the URL would serve one user's authorized Shape bytes to
the next user without `authorize()` running — and `vary: authorization`
only protects deployments that carry the credential in the
`Authorization` header (not cookie or query-token auth).

A Shape opts into shared (`public`) caching with `cacheable: true`. Only
do this when the Shape's bytes are safe for any holder of the URL: it
carries no per-user data, OR it is delivered via short-lived signed
URLs, OR auth is in the `Authorization` header. Live responses are
always `no-store`.

## Rules

- Raw `_electrolite_log` is private.
- Authorization happens before SQLite is opened.
- SQLite and Electrolite internals are not serialized into HTTP error
  bodies.
- Public/object-store data must be authorized shape output, not global
  database changes.
- Snapshot/replay responses are `private` by default; `public` caching
  is per-Shape opt-in and must only cover data safe for any URL holder.
- Shape handles include auth scope.
- Column allowlists are required.
- Delete messages must not reveal rows that were never visible to that
  authorized shape.
- Signed URLs should be short-lived for sensitive data.
