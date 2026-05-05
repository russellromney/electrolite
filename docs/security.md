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

## Signed Shape URLs

For CDN or object-store delivery, the app can mint signed shape URLs.
The signature covers:

- shape handle
- auth scope
- columns
- params
- schema version
- expiration
- retention generation

## Rules

- Raw `_electrolite_log` is private.
- Public/object-store data must be authorized shape output, not global
  database changes.
- Shape handles include auth scope.
- Column allowlists are required.
- Delete messages must not reveal rows that were never visible to that
  authorized shape.
- Signed URLs should be short-lived for sensitive data.
