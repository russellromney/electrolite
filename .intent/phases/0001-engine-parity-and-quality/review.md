# Review

Phase: 0001 — Engine wire parity, robustness, and conformance harness

## Plan Review 1 — hostile

Reviewer reads the plan adversarially: what could ship, pass tests,
and still be broken? What did the plan miss given the conversation
that produced it?

### Gaps and weaknesses

- **G1 — Node "sort keys" is necessary but not sufficient.** Sorting
  keys at the top level is easy. But the Node engine also constructs
  predicate objects in insertion order (`{ type, column, value }`)
  and runs them through `JSON.stringify`. Even with a sorted-keys
  shim at the outermost call, nested objects keep insertion order.
  Plan says "replace the Node serializer with a sorted-keys
  canonical JSON encoder." That phrasing is right but easy to
  half-implement. **Action**: explicit acceptance test asserts that
  Node's serialized canonical JSON is byte-identical to Python's
  `json.dumps(..., sort_keys=True, separators=(",", ":"))` for a
  fixture that includes `and(eq(...), gt(...))` (deeply nested).

- **G2 — A2 "introspect column types" doesn't define the boundary.**
  When does coercion happen? The plan says "during predicate
  normalization." But predicate normalization runs at *snapshot
  time* and at *replay time* (for in-process matching) and at
  *shape-handle time* (for canonicalization). If coercion happens at
  one site but not another, the engine will silently disagree with
  itself: `eq("done", true)` could compute one handle and match a
  different set of rows. **Action**: name the single normalization
  function in each engine and assert it runs at all three sites.

- **G3 — A2 column-type introspection has a chicken-and-egg
  problem.** The user's `where: ({params}) => eq("project_id",
  params.projectId)` is called *before* we have a connection to
  `PRAGMA table_info` against. Or rather, we have the connection but
  we may not have introspected the table yet. The Python and Node
  engines do this lazily during `compile_predicate` because they
  have the `info` parameter. Rust/Go/Elixir's predicate types as
  they exist today (plain enum/struct without table info) cannot do
  this without a refactor. **Action**: the predicate normalization
  step in Rust/Go/Elixir takes a `TableInfo` parameter, same shape
  as Python/Node. Update `compile_predicate` and `normalize` to
  accept it.

- **G4 — A4 (range null returns 400) breaks the engines' current
  contract surface.** Today Rust/Go/Elixir build the predicate at
  `where_fn` time, and `where_fn` does not return Result. So an
  invalid predicate constructed in `where_fn` cannot be turned into
  a 400 from inside the predicate compiler — the error has to
  propagate up to `handle()`. Plan says "every engine returns 400."
  But the Rust API has a `Predicate` enum that allows
  `Predicate::Range { value: Json::Null }`; we cannot make that
  unrepresentable without breaking the API. **Action**: validation
  happens in `compile_predicate` and surfaces as `Error::Bad(...)`,
  which `handle()` maps to 400. Add a test asserting this.

- **G5 — A3 (Elixir TOCTOU fix) needs to define what the GenServer
  does with the subscriber if the caller dies before the
  notification arrives.** Today subscribers are added to a MapSet
  that is cleared on every `notify()`. If a Plug worker dies (Cowboy
  closes the connection), the subscriber leaks until next notify.
  Not catastrophic but real. **Action**: monitor the caller pid in
  `handle_call({:handle_initial, ...})` so the subscription is
  cleaned up on `:DOWN`.

- **G6 — A6 (clear stale `current_batch_id` on bootstrap) might
  delete an active batch.** If two processes share the same SQLite
  file (cron + web server), one process's bootstrap could clear the
  other's in-flight batch_id mid-transaction. **Action**: the clear
  only happens at engine *open*, before any other writes go through
  this connection. But a different process's `current_batch_id`
  during a concurrent open is at risk. Document: "If two processes
  open the same database, one's bootstrap may invalidate the
  other's in-flight batch. Recommended: one writer process per
  database."

- **G7 — B1 (Rust `getrandom`) doesn't say what to do when
  `getrandom` is unavailable.** On platforms without `/dev/urandom`
  (e.g., embedded), `getrandom` panics or errors. **Action**:
  document the platform requirement; engine startup fails loudly if
  randomness is unavailable.

- **G8 — B3 (replay batch hard cap) needs a wire signal.** Today,
  if replay is up-to-date, `up_to_date: true`. If the batch cap was
  hit, the response should signal "more is coming" — `up_to_date:
  false` — even if the page itself is shorter than `replay_limit`.
  **Action**: when batch extension is capped, set `up_to_date: false`
  and ensure the next replay can pick up from the truncated offset.
  Test: a 200-row batch with cap of 10 generates ≥ 20 replay calls,
  each `up_to_date: false` until the last.

- **G9 — B4 (Go `Predicate` refactor) propagates to the test
  harness.** The matrix test driver passes Go predicates as map
  values via `/_test/exec`, but it does not construct them — they
  come from server-side shape registration. So the refactor is
  contained to `engines/go/` and `engines/go/server/`. But the
  conformance harness from C1 needs to encode predicates as JSON,
  and the JSON wire format must remain unchanged. **Action**: the
  refactor is purely internal Go API; JSON marshaling of the new
  variant types must produce the same bytes as today.

- **G10 — C1 (conformance harness) doesn't define how live waits
  are tested.** Live waits are inherently timing-dependent. A JSON
  case format that says "expect this response" doesn't compose with
  "send this exec while the live request is pending." **Action**:
  cases include an explicit `pending_request` pattern: start a
  request, then send another op (exec/write_batch), then assert the
  pending request resolves with a specific body. Drop the matrix's
  current sleep-25ms hack; the harness coordinator is responsible
  for ordering.

- **G11 — C1 doesn't cover engines that haven't started.** If a
  user runs the harness without `mix deps.get` for Elixir, the
  Elixir cell silently times out. Plan mentions E9 (pre-build
  Elixir) but not the analogous case for the harness. **Action**:
  the harness has the same `ensureBuilt` step as the matrix runner.

- **G12 — C2 (predicate parity test) is described as "lives in the
  conformance harness." But the in-process matcher is reachable
  only from inside each engine's process. The harness, running
  out-of-process, cannot directly compare the matcher's output to
  the SQL output.** Two paths: (a) each engine exposes a `/_test/
  match-predicate` endpoint that runs the matcher against a fixed
  row set; (b) the harness only tests the wire-observable
  consequence (replay messages on a known log produce expected
  rows). **Action**: pick (a). Add `POST /_test/match-predicate`
  with `{predicate, rows}` to every test server. Document that this
  is a test-only endpoint gated by `ELECTROLITE_TEST_SERVER=1`.

- **G13 — D1 (SSE) doesn't say what happens when the client
  disconnects mid-stream.** With long-polling, "disconnect" is
  obvious. With SSE, the server has to detect EOF on the response
  stream and free the subscriber. **Action**: every server's SSE
  branch must register a connection-close handler that frees the
  subscriber.

- **G14 — D1 doesn't say which server sends the snapshot before
  starting to stream.** Browser opens an `EventSource`. First
  message is... a snapshot? The first replay page? **Action**:
  define the SSE protocol: first event is `event: snapshot\ndata:
  {snapshot body}\n\n`, subsequent events are `event: replay\ndata:
  {messages array}\n\n`. Heartbeat events every 15s as `event:
  ping\n\n`.

- **G15 — D2 (read connection pool) only mentions Rust + Go.** But
  the user said "we should support the PRAGMAs the user already
  has." That's broader than read pools — it's "don't override user
  PRAGMAs at all." **Action**: every engine's `bootstrap()` audit:
  remove any PRAGMA statements that override defaults. If we set
  PRAGMAs at all (e.g., `foreign_keys=ON` for safety), document
  them.

- **G16 — D3 (graceful shutdown) doesn't define what happens to a
  long-running `compact()`.** If shutdown is called mid-compact,
  do we kill it or wait? **Action**: `compact()` runs to
  completion; `shutdown_timeout_ms` only bounds live waiters.
  Document.

- **G17 — Build order risk**: A1 + A2 are atomic from the user's
  perspective (handles must match), but the plan says do them
  sequentially. If A1 lands without A2, handles match for non-
  boolean predicates but diverge for boolean ones, and the
  conformance test will be confusing. **Action**: A1 and A2 land
  together in one commit, behind one passing handle-parity test.

- **G18 — The plan calls itself "one phase" but it has 24 fixes.**
  IDD doctrine: "If a change is too large to review cleanly,
  split the phase." This phase will produce 5–8 commits and touch
  every engine. A reviewer cannot hold all of it in their head at
  once. **Action**: accept that this is a meta-phase. Each cluster
  is itself reviewable. The test of correctness is "all suites
  green after each cluster." If something slips, the broken cluster
  is identifiable.

- **G19 — Existing matrix test relies on `await sleep(25)` to
  avoid the Elixir TOCTOU race.** After A3 + G10 changes, that
  sleep is no longer needed. **Action**: remove it; if any engine
  needs the sleep to pass, that engine has a bug worth surfacing.

- **G20 — `compact()` semantics under read pool (D2) are
  unspecified.** A reader holding an old transaction snapshot might
  reference log rows that compact deletes. **Action**: compact
  takes the writer connection and uses `BEGIN IMMEDIATE`. Readers
  on WAL get consistent reads from their snapshot regardless. Test.

### What looks right

- **Cluster ordering**: A → C is correct (parity before harness).
- **Refusing to force WAL** matches the user's instruction.
- **SSE as opt-in** preserves long-poll as the contract floor.
- **C1 + C2 in the same cluster** is sound: predicate parity is a
  conformance concern, not a separate architecture concern.
- **"Don't break existing tests"** as a cross-cluster invariant.

### Verdict

- **Plan is approximately right but incomplete.** 20 gaps named
  above. Most are clarifications, not redesigns; the only real
  protocol-level change needed is **G14** (define SSE framing) and
  **G2/G3** (define where coercion lives in each engine).
- **Ready to build** after folding G1, G2, G3, G4, G6, G8, G10,
  G12, G14, G17, G19 into the plan as inline "must" items. Other
  gaps are addressable during implementation review.
- **Scope warning**: this is genuinely a meta-phase. Mitigation is
  rigorous green-suite-after-each-cluster discipline.

## Decision Round 1 — folded into plan

Inputs: G1–G20 above.

Decisions:

- **D1**: Accept G1, G2, G3, G6, G8, G10, G12, G14, G17. Implement
  during the relevant cluster. Treat as "must" not "nice to have."
- **D2**: Accept G4, G5, G7, G9, G11, G13, G15, G16, G19, G20 as
  smaller clarifications. Address during implementation.
- **D3**: Accept G18 — this is a meta-phase. Mitigate by committing
  cluster-by-cluster with all suites green between commits.

Verification: each cluster's commit message lists which gaps it
addresses. Final implementation review (after all clusters land)
walks every G1–G20 and confirms.

Verdict: ready to build.
