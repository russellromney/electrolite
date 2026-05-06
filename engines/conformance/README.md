# Conformance harness

A language-agnostic test harness that asserts every Electrolite engine
produces the same wire bytes for the same protocol-level operations.

The per-engine suites (`engines/python/test_electrolite.py`,
`engines/rust/tests/engine.rs`, etc.) still exist and cover language-
specific concerns (Elixir GenServer behavior, Rust update_hook, live
wait via local primitives). The harness here is the cross-engine
contract: same input, same output bytes, every time.

## Cases

Each `cases/*.json` file is one scenario:

```json
{
  "name": "human-readable label",
  "setup": ["CREATE TABLE ..."],
  "writes": [{"kind": "exec", "sql": "...", "args": [...]}],
  "operations": [
    {"kind": "GET", "path": "/electrolite/v1/projectTodos/p1?offset=-1"},
    {"kind": "exec", "sql": "INSERT ..."},
    {"kind": "GET", "path": "/electrolite/v1/projectTodos/p1?offset={prev.offset}&log_id={prev.log_id}&shape_handle={prev.shape_handle}"}
  ],
  "assertions": [
    {"kind": "status", "op": 0, "expect": 200},
    {"kind": "shape_handle_parity"},
    {"kind": "rows_match", "op": 0, "expect": [{"id": 1, "project_id": "p1"}]}
  ]
}
```

## Running

```sh
npm run test:conformance
```

The runner spawns each engine's test server, replays the cases, and
asserts the assertions hold for every engine *and* that the engines
agree with each other where the protocol demands it.
