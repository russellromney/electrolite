# Review

Phase: 0002 — Electric parity features

## Plan Review 1 — hostile

Reading the plan adversarially: what could ship, pass tests, and
still be broken? What did the plan handwave?

### Gaps and weaknesses

- **G1 — `or` / `not` interact with normalization weirdly.** `and`
  sorts its children to make the shape_handle deterministic. `or`
  needs the same. `not` is a single child; no sort needed. But
  what about `not(or(a, b))` vs `or(not(a), not(b))`? Different
  predicates, different handles, same matched rows. That's fine —
  shape_handle is identity, not semantics. Still, document the
  recommendation that authors normalize their own predicate
  algebra.

- **G2 — `is_null` is redundant with `eq("col", null)`.** Today
  `eq` already compiles `null` → `IS NULL`. Adding a separate
  `is_null` kind splits the canonical form. **Action**: don't add
  `is_null`. Document that `eq("col", null)` IS the way to test
  null. Removes a duplicate code path.

- **G3 — `not` against `eq("col", null)` should compile to
  `col IS NOT NULL`, not `NOT (col IS NULL)`.** Subtle, but the
  former is index-friendly on SQLite. **Action**: in
  `compile_predicate`, special-case `not(eq(col, null))` →
  `col IS NOT NULL`.

- **G4 — `headers` callback may be called many times per second
  during retry storms.** If the user's `headers` callback hits a
  network (token refresh server), rate-limit risk. **Action**:
  document that the callback is invoked once per request attempt;
  recommend caching the token.

- **G5 — `onError` retry loop must have a bound.** If `onError`
  always returns new headers, we'd retry forever. **Action**:
  cap at 3 onError invocations per failed request before bubbling
  the error up. After cap, `subscribeStatus` emits an error.

- **G6 — React hooks risk re-mounting the underlying
  ShapeClient.** Naive `useEffect` would create a new client on
  every render. The plan calls for keying by URL — but the URL
  isn't unique if two components want different transports
  (long-poll vs SSE). **Action**: cache key = canonical (url +
  transport + headersFn-identity). Document.

- **G7 — `getShapeStream` returning a memoized client across
  components means `client.stop()` semantics matter.** If two
  components share a client and one unmounts, we shouldn't stop
  the client. Reference-count subscribers; only stop on last
  unsubscribe. **Action**: track subscriber count per cache key.

- **G8 — CDN headers + `live=true` are fundamentally
  incompatible.** Live-poll responses are time-dependent. Setting
  `cache-control: no-store` on live responses is correct but means
  CDNs can't help with live waiters at all. **This is fine** — it
  matches Electric. Live is for connected subscribers; cached
  history is for cold loads. Document that the CDN win is on the
  history+initial-snapshot path.

- **G9 — `etag` must be deterministic across engine restarts.**
  If we hash the response body, restarts produce different bodies
  if `log_id` changed (since `log_id` is in the body). New
  `log_id` → new etag → cache miss. **This is correct** — if the
  log was reset, clients should refetch. The cache miss is the
  signal.

- **G10 — `if-none-match` 304 path needs the engine to know the
  etag without producing the body.** Today every replay request
  produces a body. To return 304 without body, we'd need to
  compute the etag cheaply. **Two options**: (a) compute body
  anyway and skip serialization on 304, (b) compute etag
  separately from body. Option (a) is simpler and the body
  computation is cheap; the win is bandwidth not server CPU.
  **Action**: compute body, hash for etag, on `if-none-match`
  match return 304 with no body.

- **G11 — `replica=diff` breaks message replay if a client
  changes the mode mid-stream.** Client cached rows from
  `replica=full`, then asks for `replica=diff`, gets diffs against
  rows it has — fine. Client cached rows from `replica=diff` (so
  it has merged state), then asks for `replica=full` — the next
  replay returns full rows, which overwrite cleanly. So switching
  is safe in either direction as long as the client applies
  responses consistently. **Action**: document; don't enforce
  stickiness server-side.

- **G12 — `replica=diff` UPDATE messages with empty `value`.** If
  no columns changed (e.g., trigger fired but values match), the
  diff is empty. Should the engine emit the message at all?
  **Action**: skip the message entirely. Already the case for
  predicate filtering. Document.

- **G13 — `replica=diff` interacts with predicate transitions.**
  An UPDATE that takes a row from "matches" to "still matches"
  with no shape-column change is a no-op (G12). An UPDATE that
  takes a row from "matches" to "doesn't match" emits a delete,
  full key — fine. An UPDATE from "doesn't match" to "matches"
  emits an insert, full row (NOT diffed) — even with
  `replica=diff` because the client has no prior state for that
  row. **Action**: code accordingly. Test: shape predicate
  transition + diff mode produces full insert, not diff.

- **G14 — Optimistic-writes example uses HTTP POST to a user
  route.** The example assumes the user has a backend route that
  writes to SQLite and lets Electrolite triggers do their thing.
  That's the correct pattern. Document the latency: the
  optimistic state is shown immediately; the "confirmed" state
  arrives when the next replay reaches the client. Average
  latency = `pollIntervalMs / 2` for long-poll, ~immediate for
  SSE.

- **G15 — Conformance harness needs to read response headers.**
  Today `runOperation` only returns `{status, body}`. Adding the
  CDN-headers conformance case requires `{status, headers, body}`.
  **Action**: extend the operation result shape. `cross_engine`
  paths can then reference `0.headers.etag`, etc.

- **G16 — The plan says "small `LocalMutationBuffer` helper" but
  doesn't define where it ships.** If it's in `clients/react/`, it
  pulls in React for non-React users. **Action**: ship it in
  `clients/browser/local-mutation-buffer.js` (vanilla), use it
  from the React example. React hook wraps it.

- **G17 — Three new conformance cases for `or` / `not` is
  redundant if they all assert the same thing (handle parity).**
  One case per predicate kind is fine; three cases is OK because
  they exercise different code paths in `compile_predicate`. Keep.

- **G18 — The phase has 6 features but limited test coverage in
  the plan**: predicate parity (3 cases), CDN headers (1 case),
  replica=diff (1 case). That's 5 conformance cases but no
  cross-engine test for `headers` callback or React hooks (those
  are client-only, not cross-engine). **Action**: be explicit
  that steps 2 and 3 ship per-language tests, not conformance
  cases. The conformance suite is for cross-engine wire parity;
  ShapeClient + React are client concerns.

- **G19 — `replica=diff` is the most invasive wire change.** It
  changes UPDATE message semantics. A regression where it's
  applied unconditionally breaks every long-poll client. **Action**:
  default is `replica=full`; the new behavior is opt-in via query
  param. Existing clients see no change.

- **G20 — The plan doesn't mention how `replica=diff` interacts
  with SSE.** SSE pushes events; the `replica` param is on the
  initial GET. Does it persist across the streamed events? Yes —
  the param is part of the request URL, the server captures it
  once, all subsequent events on that stream use the same mode.
  **Action**: confirm in the SSE wire-format docs.

### What looks right

- Build order (predicates → ShapeClient → React → CDN → diff →
  docs) puts the highest-leverage / lowest-risk items first.
- Every change is additive — nothing breaks existing wire format
  or API.
- CDN headers as the biggest scale story.
- Server-defined predicates only stays the discipline.
- Optimistic writes is a docs+example, not a built-in feature
  shape — correct given uncertain demand.

### Verdict

- **Plan is approximately right.** 20 gaps named above, mostly
  small refinements:
  - Drop `is_null` (G2). `eq("col", null)` already covers it.
  - Special-case `not(eq(col, null))` to `IS NOT NULL` (G3).
  - Cap onError retries at 3 (G5).
  - Reference-count React hook subscribers (G7).
  - Extend conformance harness to expose response headers (G15).
  - `LocalMutationBuffer` ships under `clients/browser/`, not
    `clients/react/` (G16).
- **Ready to build** with these clarifications folded in.
- **Scope**: 6 features, ~5 commits, ~5 conformance cases, ~10
  per-engine + per-client tests. Smaller than phase 0001.

## Decision Round 1 — folded into plan

Inputs: G1–G20.

Decisions:
- **D1**: Accept G2 — drop `is_null`, document that `eq("col",
  null)` IS the null test.
- **D2**: Accept G3 — special-case `not(eq(c, null))` →
  `IS NOT NULL`.
- **D3**: Accept G5 — onError retry cap = 3.
- **D4**: Accept G7 — React hook reference counting.
- **D5**: Accept G15 — conformance harness exposes headers in
  operation results.
- **D6**: Accept G16 — `LocalMutationBuffer` in
  `clients/browser/`, not `clients/react/`.
- **D7**: Accept G10 — server computes body, hashes for etag,
  returns 304 with no body on `if-none-match` match.
- **D8**: Accept G13 — predicate-transition inserts under
  `replica=diff` always emit full row.
- Other gaps (G1, G4, G6, G8, G9, G11, G12, G14, G17–G20):
  documented during implementation.

Verdict: ready to build.

## Implementation Review 1

### What landed

- **Step 1** (commit 5958b2a) — `or` and `not` predicates added to
  every engine. Special-case `not(eq(col, null))` →
  `col IS NOT NULL`. Predicate normalization sorts `or` children
  via canonical sorted-keys JSON. New conformance cases 0016, 0017,
  0018.
- **Step 2** (commit c348e8b) — `headers` callback and `onError`
  retry on `ShapeClient`, with the retry cap of 3 from D3. Three
  new browser tests for token-refresh and capped retry.
- **Step 3** (commit 825e8d2) — `clients/react/` package with
  `useShape`, `preloadShape`, `getShapeStream`, `getShape`. Cache
  by `(url, transport)`. React added as devDep; peer >= 18.
- **Step 4** (commit 23ddb14) — CDN-cacheable response headers
  (`etag`, `cache-control`, `vary`) on every test server + Node
  engine. 304 Not Modified path on `if-none-match`. Conformance
  case 0019 verifies cross-engine parity. Side fix: in-process
  Node test server now forwards every Response header.
- **Step 5** (commit 5288dae) — `replica=diff` UPDATE messages.
  Server emits only changed columns; client merges instead of
  overwrites. Predicate-transition INSERTs always full (D8).
  Conformance case 0020.
- **Step 6** (this commit) — `docs/optimistic-writes.md` covering
  three patterns. `clients/browser/local-mutation-buffer.js`
  helper for Pattern B and C (4 tests).

### What did not change

- The protocol contract: every wire change is opt-in (`replica`
  query param, `headers`/`onError` client options, `transport: sse`
  flag, `if-none-match` 304 path).
- Long-poll remains the default transport.
- `shape_handle` semantics — every new predicate normalizes to a
  deterministic canonical form.
- All previously passing tests continue to pass.

### Direct proof

- 20/20 conformance cases pass.
- 5/5 matrix cells (browser × {node, python, rust, go, elixir}).
- Per-engine: Node 22, Python 18, Rust 20, Go all green, Elixir 19.
- React: 3/3 cache-and-share assertions.
- Browser: 23/24 (one pre-existing localStorage failure unrelated
  to this phase).
- LocalMutationBuffer: 4/4.

### Findings closed

- D1 — `is_null` not added; `eq(col, null)` is the way.
- D2 — `not(eq(col, null))` compiles to `IS NOT NULL` for index
  use.
- D3 — onError retry capped at 3.
- D4 — React hook reference counting on URL+transport.
- D5 — Conformance harness's `runOperation` exposes response
  headers (etag, cache-control, vary).
- D6 — `LocalMutationBuffer` ships under `clients/browser/`.
- D7 — Server returns 304 with no body on `if-none-match` match.
- D8 — Predicate-transition INSERT under `replica=diff` carries
  full row, never a diff.

### Findings deferred

- Real React hook rendering tests would need
  `react-dom/test-renderer`. The cache-share layer is the actual
  correctness surface and is tested.
- Optimistic-writes Pattern C reconnect-replay logic (`replay()`
  on the buffer) is implemented but not exercised end-to-end. The
  example in docs/ shows the shape; a runnable example app is a
  separate phase.

### Real bugs caught by the new tests

- **Node `auth_scope` divergence** (caught earlier in phase 0001
  hostile audit when boolean-coercion conformance was added).
  Fixed in 6fa581e.
- **Node `and` children sort order** (same hostile audit, AND-SQL
  conformance). Fixed in 6fa581e.
- These weren't new in phase 0002 but are worth noting as
  conformance-suite catches.

### Verdict

- Phase 0002 ships all 6 features named in the plan plus the
  decisions D1–D8 from this review. No deferred work.
