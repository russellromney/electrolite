# ROADMAP

Roadmap items are future work. Completed work lives in
[CHANGELOG.md](CHANGELOG.md).

## North Star

Electrolite is a TypeScript-first embedded SQLite sync layer. A Node app
owns SQLite, auth, writes, and an Electrolite HTTP endpoint; browsers get
an initial authorized Shape snapshot and then live logical changes.

```text
SQLite + generated triggers
  -> durable logical change log
  -> app-embedded Shape endpoint
  -> browser client consumes snapshot + offset replay
```

## Near-Term: Make The Demo Feel Real

These are the highest-leverage items before showing this widely.

- React hooks over `ShapeClient`.
- A tiny npm-free starter template with server route, browser client, and
  SQLite setup in one folder.
- A visible unauthorized panel in the demo showing denied Shapes return
  `404 shape_not_found`.
- Composite-primary-key demo row so people see it is not toy-key-only.
- README benchmark numbers for snapshot, replay, and live fanout.

## Internals: Keep Semantics Sharp

- Add `explainShape(...)` diagnostics:
  - table
  - selected columns
  - key columns
  - original predicate
  - normalized predicate
  - trigger installation status
  - suggested SQLite indexes
- Add retention auto-compaction with safe per-table defaults.
- Add a public benchmark harness for:
  - snapshot 1k rows
  - replay one change
  - live fanout across 100 waiting clients
  - mixed shared/private Shapes
- Add server-side stats:
  - active live subscribers
  - active shape handles
  - wakeups
  - replay rows scanned
  - replay messages emitted

## Predicate And Shape Scale

- Keep the fast path focused on simple, indexable predicates:
  - `column = value`
  - `column IN (...)`
  - simple ranges later
- Add a Shape predicate index in the Node implementation so writes can
  find candidate Shapes before exact membership evaluation.
- Keep arbitrary SQL out of the browser protocol.
- Add diagnostics that tell users which SQLite indexes their Shapes want.
- Add a strategy for large `IN` lists: registered server helpers, signed
  Shape tokens, or POST-based app routes.

## Browser Client

- React hooks.
- IndexedDB schema versioning and cache eviction policy.
- Multi-tab leadership hardening for backgrounded tabs.
- Dedicated browser package publishing when npm packaging starts.
- Adapters for Solid, Svelte, and vanilla stores later.

## Fanout And Caching

This only matters once shared Shapes have real traffic.

- Coalesce identical in-process waits by `shape_handle + offset`.
- Materialize immutable response chunks by Shape handle and offset.
- Make historical chunks cacheable with immutable cache headers and ETags.
- Keep live delivery as HTTP long-polling; WebSockets can be an adapter.
- Explore CDN request collapsing for identical shared Shape waits.

## Optional Object Storage Mode

S3/Cinch-style object storage may be useful for immutable authorized Shape
chunks.

- Store chunks under opaque Shape handles.
- Keep raw logs private.
- Serve chunks through the app, a private proxy, or signed URLs.
- Do not infer Shape semantics from SQLite pages; Shape semantics come
  from the trigger log and server-defined predicates.

## Later

- Precomputed per-Shape logs.
- Offline writes and conflict handling as a separate track.
- Replica placement for read-heavy apps, if the simple embedded model is
  no longer enough.
