# Plan

Phase: 0001 — Engine wire parity, robustness, and conformance harness

## What we are building

A single phase that takes Electrolite from "five engines that pass
hand-translated tests" to "five engines that demonstrably produce
identical wire bytes, fail loudly on bad input, and are validated by a
language-agnostic conformance harness." Concretely, 24 issues from a
hostile code review of the current state, grouped into five clusters.
We do them as one phase because the wire-parity fixes (cluster A) are
prerequisites for the conformance harness (cluster C), and the
harness is what makes the rest reviewable.

## Clusters

### Cluster A — Wire parity (cross-engine `shape_handle` must match)

- **A1 (was #1)**: Node `shape_handle` uses `JSON.stringify` which
  does not sort keys. Every other engine canonicalizes with sorted
  keys. Fix: replace the Node serializer with a sorted-keys canonical
  JSON encoder in `electrolite-node-engine.ts`.
- **A2 (was #3)**: Boolean predicate values are coerced to `0/1` in
  Python and Node (when the column has `BOOLEAN` affinity) but not in
  Rust/Go/Elixir. Fix: every engine introspects column types via
  `PRAGMA table_info` during predicate normalization and coerces
  `true ↔ 1`, `false ↔ 0`, `null ↔ null`. Type-safe in both
  directions.
- **A3 (was #4)**: Elixir live wait has a TOCTOU race between
  `:handle_initial` reply and `:subscribe_change`. Fix: register the
  subscriber inside the same `handle_call({:handle_initial, ...})`
  reply using the calling pid, atomically with returning `{:wait,
  ...}`.
- **A4 (was #6)**: `range` predicate with `null` value is
  inconsistent — Python raises (500), Node throws, Rust/Go/Elixir
  emit `0` (silent empty). Fix: every engine returns 400
  `bad_predicate` (or equivalent) when a range predicate is
  constructed with null.
- **A5 (was #5)**: Bad `offset` query param crashes Python and
  Elixir. Fix: parse integers safely; on failure, return 400
  `bad_request`.
- **A6 (was #22)**: `current_batch_id` meta row can survive a
  crashed `write_batch`, tagging the next unrelated write with the
  old batch. Fix: clear `current_batch_id` during `bootstrap()` in
  every engine.

### Cluster B — Engine robustness

- **B1 (was #7)**: Rust's `random_hex` is xorshift seeded by wall
  time. Fix: switch to `getrandom` (small dep) for `log_id` and
  `batch_id`.
- **B2 (was #18)**: Rust uses `unwrap()` for every JSON parse from
  SQLite log rows. Fix: propagate as `Error::Bad(...)`; corrupted log
  rows return `500` instead of panicking.
- **B3 (was #13)**: Replay batch-extension has no upper cap. A
  single 10M-statement `write_batch` would force replay to load all
  10M rows. Fix: cap extension at `10 × replay_limit`. If a batch is
  bigger, return what fits and let the next replay continue.
- **B4 (was #19)**: Go `Predicate` is a single struct with `Value`,
  `Values`, `Predicates` and `omitempty`. Refactor: sealed-interface
  pattern with variant types (`AllPredicate`, `EqPredicate`, etc.).
  Update all callsites in `engines/go/` and `engines/go/server/`.

### Cluster C — Language-agnostic conformance harness

- **C1 (was #24)**: Replace the hand-translated conformance suites
  with one source of truth: a JSON script of operations + expected
  outputs replayed against every engine.
  - Format: a directory `engines/conformance/cases/` of `*.json` test
    cases. Each case has setup SQL, a list of operations (snapshot /
    replay / live / exec / write_batch / compact), and expected
    response JSON (or wildcards for fields like `log_id`).
  - Runner: a small `engines/conformance/run.ts` (Node) that spawns
    each engine's HTTP server (matrix-style) and replays the cases.
    Asserts byte-equal responses across engines for fields that
    should be identical (`shape_handle`, `key_columns`, `messages
    [].type`, `messages[].batch_id` matched as opaque).
  - Cases cover at minimum: every test currently in PROTOCOL.md plus
    a new "shape_handle parity" case that asserts byte-identical
    handles across all engines.
- **C2 (was #16)**: Predicate parity property test (SQL eval vs
  in-process matcher). Lives in the conformance harness as a fixed
  fixture: a row set, a list of predicates, expected matching IDs.
  Each engine answers the same question two ways (SQL `WHERE` for
  snapshot, in-process matcher for replay). Both must return the
  same set.
- After C1 lands, the language-specific conformance suites (Python
  `test_electrolite.py`, Rust `tests/engine.rs`, etc.) shrink to
  language-specific concerns only (live wait via local primitives,
  GenServer behavior in Elixir, etc.).

### Cluster D — Transport and concurrency

- **D1 (was #14)**: SSE transport. Add an alternative HTTP route
  family (`?transport=sse` query flag, `Accept: text/event-stream`
  header, or a separate `/electrolite/v1/sse/...` prefix — pick one
  in design). Server holds one connection per subscriber and pushes
  events as `data: {json}\n\n` framing. Long-polling stays as the
  default for backwards compatibility.
  - Browser client: `ShapeClient` gains an option `transport: "long
    -poll" | "sse" | "auto"` (default `"long-poll"` for now). With
    `"sse"`, the client uses an `EventSource`.
  - Reference: Electric Sync supports long-poll and SSE. Their
    framing is `event: ...\ndata: {json}\n\n`. We mirror.
- **D2 (was #11)**: Concurrency. The user wants engines to respect
  user-set PRAGMAs rather than forcing WAL. Fix:
  - Bootstrap does NOT issue `PRAGMA journal_mode=WAL`. If the user
    wants WAL, they set it themselves before opening the engine.
  - Document the PRAGMA recommendations (`journal_mode=WAL`,
    `synchronous=NORMAL`, `busy_timeout=5000`) in each engine README.
  - Add a `read_connection_pool_size` option (default 1) to engines
    that can support it (Rust + Go are the realistic candidates).
    When >1, snapshot/replay reads use a pool of read-only
    connections; writes still go through the single writer
    connection. Live waits use the writer's `update_hook` for wake.
  - Python and Elixir keep single-connection (their bindings don't
    cleanly expose a read-only pool with low effort).
- **D3 (was #23)**: Graceful shutdown. Add `engine.shutdown()` to
  every engine. Behavior:
  - Stop accepting new requests.
  - For each in-flight live waiter, return a `200 {messages: [],
    up_to_date: true, shutdown: true}` response so the client
    re-connects on the next snapshot.
  - Wait up to a configurable `shutdown_timeout_ms` (default 1000)
    for in-flight requests to finish.
  - Close the SQLite connection.

### Cluster E — Quality of life

- **E1 (was #2)**: Cross-engine `shape_handle` equality test. Lives
  in the conformance harness from C1. Mentioned here for tracking.
- **E2 (was #8)**: Test servers refuse to start unless
  `ELECTROLITE_TEST_SERVER=1` is in env. Closes the
  unauthenticated-SQL-exec exposure.
- **E3 (was #9)**: Document `update_hook`'s in-process-only scope in
  the Rust README's "Live wait" section.
- **E4 (was #10)**: Matrix test calls `client.stop()` in a `finally`
  block. No more leaked timers.
- **E5 (was #12)**: IN-predicate dedup numeric edge case (`1` vs
  `1.0`). **Deferred.** Add a paragraph to PROTOCOL.md noting the
  limitation.
- **E6 (was #15)**: Document Elixir's lack of `update_hook` and what
  user code patterns are unsafe.
- **E7 (was #17)**: Replace the Go cute trick `strings.Repeat(",?",
  n)[1:]` with a clearer version.
- **E8 (was #20)**: Rust binary path inconsistency. Make the matrix
  test driver search both `engines/rust/target/debug/...` and the
  workspace root `target/debug/...`.
- **E9 (was #21)**: Pre-build Elixir deps in `ensureBuilt()` so
  the first matrix run is not 30s slower.

## What will not change

- **The protocol contract** in `PROTOCOL.md` stays the same. We are
  enforcing it more strictly, not changing it. Exception: SSE adds
  optional transport to the contract; long-polling still works.
- **Browser-client API surface.** `new ShapeClient(url, opts)` still
  works exactly as today. SSE is opt-in via the `transport` option.
- **Existing tests.** Every test currently passing must still pass.
  The conformance harness *replaces* hand-translated suites only
  after the harness covers the same cases.
- **No new dependencies for the small engines beyond what is needed.**
  Rust gets `getrandom`. Go gets nothing new. Elixir gets nothing new.
  Python gets nothing new. Node gets nothing new.
- **No engine-side WAL forcing.** User's existing PRAGMAs win.

## How we will build it

Build order, executed left-to-right:

1. **Cluster A** in this order: A1 (Node sort keys) → A2 (boolean
   coercion alignment) → A3 (Elixir TOCTOU) → A4 (range null) → A5
   (bad offset) → A6 (stale batch_id). Run all conformance suites
   plus matrix after each.
2. **Cluster B**: B1 → B2 → B3 → B4. Run Rust + Go suites + matrix.
3. **Cluster C**: build the conformance harness and the case format,
   port the existing PROTOCOL cases into JSON, add the cross-engine
   `shape_handle` equality case (E1) and the predicate-parity case
   (C2). Wire `npm run test:conformance`. Once the harness covers a
   case, remove the equivalent hand-translated test.
4. **Cluster D**: D1 (SSE) → D2 (read pool option, no PRAGMA forcing)
   → D3 (graceful shutdown). SSE needs a new client transport mode.
5. **Cluster E**: E2 → E4 → E7 → E8 → E9 → E3, E6 (docs) → E5
   (defer note). Mostly small.

Each cluster is committed as one or more focused commits. Total
estimate: 5–8 commits.

## How we will prove it works

- **Direct proof per item**:
  - A1: a new test that registers the same shape on every engine and
    asserts `shape_handle` bytes are identical.
  - A2: a test in the harness that constructs `eq("done", true)`
    against a `BOOLEAN` column and asserts the predicate normalizes
    to `eq("done", 1)` on every engine, with identical handle.
  - A3: a stress test that races writes and live requests on Elixir;
    no live request should miss its wake.
  - A4: harness asserts every engine returns 400 for `gt("x",
    null)`.
  - A5: harness asserts every engine returns 400 for
    `?offset=banana`.
  - A6: kill an engine mid-`write_batch` (or simulate by setting
    `current_batch_id` then re-bootstrapping); the next write must
    have a fresh batch_id.
  - B1: Rust test asserts two `Electrolite::open` calls within the
    same nanosecond produce different `log_id`s. Stress with 1000
    iterations.
  - B2: feed a corrupted log row, assert engine returns an error
    response not a panic.
  - B3: insert 200 rows in a single `write_batch`, replay with
    `limit=1`, assert response contains at most `10 × limit` rows;
    next replay continues.
  - B4: Go callsites compile against the new sealed types; old
    constructor patterns are replaced.
  - C1/C2: the conformance harness itself runs and is committed; the
    matrix and per-engine suites still pass.
  - D1: a new matrix scenario uses the SSE transport against every
    engine and observes the same materialization as long-poll.
  - D2: `PRAGMA journal_mode` set to WAL before `open()` survives
    bootstrap; engines do not issue their own PRAGMAs. Read pool
    test on Rust: 100 concurrent snapshots while one writer commits
    every 50ms — no errors, no corruption.
  - D3: shutdown test — start engine, register 5 live waiters, call
    `shutdown()`, assert all 5 receive a clean response within
    `shutdown_timeout_ms`.
  - E series: each is small enough that a passing build + manual
    check is enough.
- **Regression / blast-radius**:
  - All five per-engine conformance suites still pass.
  - The matrix (5 engines × 1 client) still passes.
  - `npm test` (browser + node + python) still passes.
  - `npm run test:all` adds rust + go + elixir + matrix +
    conformance and all green.

## How we will prove we did not break earlier intent

- The Node `ShapeClient` integration tests in
  `packages/electrolite-node/electrolite-node.test.ts` continue to
  pass without modification. They are the user-shaped end-to-end
  proof that the engine handle / browser client interaction is
  intact.
- The matrix test (browser client × every engine over real HTTP) is
  the strongest blast-radius signal. If any cluster breaks any
  engine's HTTP wire format, the matrix fails.
- The `compactLogToLastForTable` Node API (used by the existing 409
  resync test) keeps working unchanged.
- `clients/browser/electrolite.test.ts` continues to pass; the
  multi-tab leadership / IndexedDB persistence tests are not
  touched.

## Files likely to change

- `packages/electrolite-node/electrolite-node-engine.ts` (A1, A2,
  A6, D1, D3)
- `packages/electrolite-node/electrolite-node.ts` (D1, D3)
- `engines/python/electrolite.py` (A2, A4, A5, A6, D1, D3)
- `engines/python/server/server.py` (D1, E2)
- `engines/rust/src/lib.rs` (A2, A4, A6, B1, B2, D1, D2, D3, E3)
- `engines/rust/Cargo.toml` (B1)
- `engines/rust/server/main.rs` (D1, E2)
- `engines/go/electrolite.go` (A2, A4, A6, B4, D1, D2, D3, E7)
- `engines/go/server/main.go` (B4, D1, E2)
- `engines/elixir/lib/electrolite.ex` (A2, A3, A4, A5, A6, D1, D3,
  E6)
- `engines/elixir/lib/test_server.ex` (D1, E2)
- `clients/browser/electrolite.js` (D1)
- `tests/matrix.test.ts` (E4, E8, E9, plus SSE matrix scenario)
- `engines/conformance/` (new — C1, C2, E1)
- `engines/PROTOCOL.md` (A4 wording, E5 note)
- `engines/README.md` (parity matrix update)
- READMEs per engine (PRAGMA recommendations, E3, E6)
- `package.json` (`test:conformance` script)

## Areas that should not be touched

- KuzuDB-style cross-database concerns. We do not introduce them.
- `clients/browser/electrolite.js` IndexedDB / multi-tab logic.
  D1 (SSE) only adds a new transport branch; the persistence and
  leadership code is not touched.
- The existing `ShapeClient` API for long-polling. Long-poll stays
  the default, opt-in to SSE.
- Examples (`examples/basic-todos/`, etc.) — those exercise the
  public surface and should still work.

## Assumptions and risks

- **A2 risk**: introspecting column types in three more engines
  could expose edge cases (composite types, weird affinity decls).
  Test against the existing `todos.done BOOLEAN NOT NULL DEFAULT 0`
  and at least one variant.
- **C1 risk**: the conformance harness adds a Node-side dependency
  to run, and the spawn-five-subprocesses pattern is slow in CI.
  Mitigation: gate by `npm run test:conformance` (not part of `npm
  test`).
- **D1 risk**: SSE adds protocol surface that all engines must
  implement consistently. If we ship SSE half-baked the parity story
  weakens. Mitigation: SSE is opt-in; long-poll remains the default
  and remains the contract floor.
- **D2 risk**: read connection pools introduce cache-coherence
  questions (a snapshot taken on a reader may pre-date a write
  observed by another reader). Mitigation: every snapshot's `offset`
  is the pinning truth; clients never compare offsets across
  shapes.
- **D3 risk**: graceful shutdown requires every engine to track
  live waiters explicitly. Most already do (subscribe pattern).
  Elixir's pattern after A3 makes this clean; Rust/Go need explicit
  waiter sets.
- **B4 risk**: Go callers in user code break. Acceptable —
  Electrolite is pre-1.0 and there are no external users yet.

## Commands

- `npm test`
- `npm run test:all`
- `npm run test:matrix`
- `npm run test:conformance` (new)
- per-engine: `npm run test:python` / `:rust` / `:go` / `:elixir` / `:node`

## Notes

- Cluster ordering matters because A is a prerequisite for C
  (the harness asserts byte-equal handles, so handles must agree
  first). D (transport) is independent of A but built last because
  SSE on top of an unstable handle would compound risk.
- Each cluster ends with all suites green before the next starts.
