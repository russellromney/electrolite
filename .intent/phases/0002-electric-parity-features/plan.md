# Plan

Phase: 0002 — Electric parity features

## What we are building

Six concrete features that close the most meaningful gaps between
Electrolite and ElectricSQL, in the order they'll be built:

1. **`or` / `not` / `is_null` predicates** — extends the predicate
   language with the three operators that cover the most real-world
   missing cases. Server-defined only; no SQL-injection surface
   change.
2. **`headers` callback + `onError` retry on `ShapeClient`** — lets
   the browser client refresh auth tokens between requests and
   recover from transient errors without manual reconnect logic.
3. **`clients/react/` package** with `useShape`, `preloadShape`,
   `getShapeStream`, `getShape`. Standard React adoption surface,
   thin wrapper over the existing `ShapeClient`.
4. **CDN-cacheable response headers** — every snapshot/replay
   response carries `etag`, `cache-control`, `vary: authorization`.
   Replays with `offset >= 0` are cached as immutable. Clients
   sending `if-none-match` get `304 Not Modified`. This is the
   biggest scale-story change in the phase.
5. **`replica=diff` UPDATE messages** — a query-param mode that
   makes UPDATE messages carry only changed columns. Browser
   materializer merges instead of overwrites. Significant bandwidth
   cut for wide tables.
6. **Optimistic-writes pattern documented** — a worked example
   showing the simplest pattern (local mutation buffer + REST POST
   to a user route + reconciliation when the next replay arrives).
   No new client API; just a guide and a runnable example.

## What will not change

- Long-poll stays the default transport; SSE stays opt-in.
- Server-defined shapes only. The `where` clause is computed by the
  app's `where_fn`, never parsed from a client query string.
- Existing `ShapeClient` constructor signature is additive only —
  every option that exists today still works the same way.
- The protocol contract in `engines/PROTOCOL.md` only grows; no
  existing wire bytes change.
- All 15 conformance cases still pass after each step.
- All 5×2 matrix cells still pass.

## How we will build it

Build order:

1. **Predicates `or` / `not` / `is_null`.**
   - Extend `Predicate` enum in Rust, sealed-interface in Go,
     dataclass in Python, atom-tagged map in Elixir, type union
     in Node.
   - `compile_predicate`: `or` → `(c1) OR (c2)`, `not` → `NOT (c)`,
     `is_null` → `col IS NULL`.
   - `predicate_matches`: parallel in-process implementations.
   - `predicate_to_json` + normalization: sort `or` children
     identically across engines (canonical sorted-keys JSON, like
     `and`).
   - Parser update in `predicate_from_json` / `PredicateFromJSON` /
     `predicate_from_json` for the conformance harness.
   - Three new conformance cases: `or` parity, `not` parity,
     `is_null` parity.

2. **`headers` callback + `onError` retry on browser `ShapeClient`.**
   - New options: `headers?: () => Headers | Promise<Headers>`,
     `onError?: (error, attempt) => Promise<{headers?, params?} |
     undefined>`.
   - Each `fetch()` and `streamSse()` call resolves headers before
     dispatching.
   - On a 4xx/5xx that isn't a 409, `onError` is called; if it
     returns new headers/params, retry once with those.
   - Existing retry/backoff for transient failures stays.
   - New `clients/browser/electrolite.test.ts` cases: token-refresh
     on 401, recovery from transient 500.

3. **`clients/react/` package.**
   - New directory `clients/react/` with `electrolite-react.tsx`
     exposing `useShape`, `preloadShape`, `getShapeStream`,
     `getShape`.
   - Package.json with `peerDependencies: { react: ">=18" }`.
   - Internal store keyed by canonical URL so multiple components
     share one stream per shape.
   - Smoke test using React's `act` API in node:test.

4. **CDN-cacheable response headers.**
   - Every engine's `handle()` (or its HTTP wrapper in test
     servers) now emits:
     - `etag: "<shape_handle>-<offset>"` on 200 responses.
     - `cache-control: max-age=31536000, immutable` for replay
       responses with `offset >= 0` and `up_to_date == true`.
     - `cache-control: max-age=5` for snapshot (offset=-1)
       responses — short window so app data isn't massively stale.
     - `cache-control: no-store` for `live=true` and SSE.
     - `vary: authorization` always.
     - `304 Not Modified` (with no body, just headers) when the
       client sends `if-none-match` matching the response we'd
       compute.
   - Conformance case 0016: every engine emits the same etag /
     cache-control for the same request.
   - Per-engine test: 304 path returns no body and matches.

5. **`replica=diff` UPDATE messages.**
   - Wire format addition: optional query param `replica=diff`
     (default `replica=full`). Documented in PROTOCOL.md.
   - Engine change: when `replica=diff`, UPDATE messages emit
     `value` containing only keys whose value changed since
     `old_json`.
   - Browser `ShapeClient` materializer: when applying UPDATE,
     merge `value` into existing row instead of overwriting.
     Behavior is unchanged for `replica=full` (the default).
   - New `ShapeClient` option: `replica?: "full" | "diff"`.
   - Conformance case 0017: `replica=diff` produces only changed
     columns in UPDATE `value`, identical bytes across engines.

6. **Optimistic-writes pattern documentation.**
   - New `docs/optimistic-writes.md` covering:
     - Pattern A: online REST (no optimism, the simplest baseline).
     - Pattern B: local optimistic state via `useOptimistic`-style
       hook, reconciles when next replay confirms or contradicts.
     - Pattern C: persisted local mutation buffer (survives reload,
       replays on reconnect).
   - Worked example: `examples/optimistic-todo/` — a minimal todo
     app demonstrating Pattern B against the Node engine.
   - No new shipping client code beyond what Pattern B requires
     (small `LocalMutationBuffer` helper).

## How we will prove it works

- **Predicate parity**: 3 new conformance cases (`or`, `not`,
  `is_null`). Per-engine cases also for the in-process matcher.
- **`headers` / `onError`**: 2 new browser client tests (401 →
  refresh + retry, 500 → backoff + onError → retry).
- **React hooks**: smoke test that mounts a component using
  `useShape`, runs against an in-process Node engine, asserts
  `data` updates as writes happen.
- **CDN headers**: conformance case 0016 asserts every engine emits
  the same `etag` and `cache-control` for the same shape+offset.
  Per-engine test for the 304 path.
- **`replica=diff`**: conformance case 0017 asserts UPDATE `value`
  contains only changed columns and matches across engines.
- **Optimistic-writes example**: integration smoke that runs the
  example, performs a write, asserts the local optimistic state
  matches the eventually-replayed state.

## How we will prove we did not break earlier intent

- All 15 existing conformance cases still pass.
- All 5×2 matrix cells (long-poll + SSE × 5 engines) still pass.
- `npm run test:all` is green.
- Per-engine suites pass: Node 22, Python 18, Rust 20, Go all,
  Elixir 19 (or higher with the new tests).

## Files likely to change

### Predicates (step 1)
- `engines/python/electrolite.py`
- `engines/rust/src/lib.rs`
- `engines/go/electrolite.go`
- `engines/elixir/lib/electrolite.ex`
- `packages/electrolite-node/electrolite-node-engine.ts`
- `packages/electrolite-node/electrolite-node.ts`
- `engines/conformance/cases/0016-or-predicate.json`
- `engines/conformance/cases/0017-not-predicate.json`
- `engines/conformance/cases/0018-is-null-predicate.json`
- per-engine test files (small additions)
- `engines/PROTOCOL.md`

### `headers` / `onError` (step 2)
- `clients/browser/electrolite.js`
- `clients/browser/electrolite.test.ts`

### React (step 3)
- `clients/react/package.json`
- `clients/react/electrolite-react.tsx` (or `.ts`)
- `clients/react/electrolite-react.test.ts`
- `engines/README.md` (mention React surface)

### CDN headers (step 4)
- `packages/electrolite-node/electrolite-node.ts` (set headers on
  Response)
- Each engine's test server (`engines/python/server/server.py`,
  etc.) sets headers based on what `handle()` returns.
- `engines/conformance/run.ts` (the harness ignores headers today;
  a new op kind to fetch headers).
- `engines/conformance/cases/0019-cdn-headers.json`
- `engines/PROTOCOL.md`

### `replica=diff` (step 5)
- Each engine's `handle()` reads the `replica` param and threads it
  into `replay()`.
- Each engine's `messages_for` builds a diff'd value when
  `replica=diff`.
- `clients/browser/electrolite.js` materializer: merge instead of
  overwrite when `replica=diff` was requested.
- `engines/conformance/cases/0020-replica-diff.json`
- `engines/PROTOCOL.md`

### Optimistic writes (step 6)
- `docs/optimistic-writes.md`
- `examples/optimistic-todo/` (new)

## Areas that should not be touched

- The wire format for existing requests/responses. Every change in
  this phase is additive — new query params, new headers, new
  predicate kinds. No breaking changes.
- The auth model. `authorize` callback stays. `headers` callback
  on the client side is purely for the request layer (token
  refresh).
- The conformance harness's case format for existing cases. New op
  kinds are additive.
- The `engines/PROTOCOL.md` semantics for `shape_handle`. We add
  predicate kinds; the canonical-JSON serializer already handles
  any well-formed predicate object.
- The IDD framework files (`SYSTEM.md`, prior phase artifacts).

## Assumptions and risks

- **`replica=diff` makes the wire format mode-dependent.** A client
  with `replica=diff` materialized rows over time MUST consistently
  request `replica=diff` on subsequent replays, because the diffs
  are relative. Mitigation: client option is sticky for the
  ShapeClient instance. Document.
- **CDN cache headers depend on offsets being deterministic.** They
  are: replay with `offset_in >= 0` is content-addressed by
  `(shape_handle, offset_in)` and the messages are immutable
  segments of the log. Compaction deletes them server-side but the
  cached body remains valid for the client that has it.
- **React hooks add a peerDependency.** First time the repo touches
  React. The package is `peerDependencies` not `dependencies`, so
  installs without React don't pull it in. Tests for the hook may
  require `react-test-renderer` or `@testing-library/react`; we'll
  use `react-test-renderer` if needed (zero-dep alternative is
  inline `act` from React 18).
- **Predicate `not` against an indexed column may table-scan.**
  Document the index recommendation. SQLite typically can use the
  index for negation against a small set, but pathological cases
  exist. Add to the diagnostics roadmap, not blocking.

## Commands

- `npm test`
- `npm run test:all`
- `npm run test:matrix`
- `npm run test:conformance`
- per-engine: `npm run test:python` / `:rust` / `:go` / `:elixir` /
  `:node`

## Notes

- Step ordering matters because step 4 (CDN headers) depends on
  every engine having stable response shape, which steps 1–3 don't
  affect; and step 5 (`replica=diff`) is the most invasive wire
  change so it's done last.
- Each step ends with all suites green before the next starts.
- This phase is intended to ship as 6 commits, one per step, plus
  a closing SYSTEM.md update.
