# Optimistic writes with Electrolite

Electrolite is a read-side sync engine: the browser client materializes
authorized rows from your SQLite log. It does **not** send writes back
to the server — your app does that, through its own routes.

That's a feature, not a gap. It keeps the protocol small and the
auth boundary obvious. But you need a write path. Three patterns,
in order of complexity.

## Pattern A — Online REST (the baseline)

The simplest: every write is an HTTP request to your backend, which
runs `engine.execute(...)` against SQLite. The trigger fires, the
log gets a row, and the browser client picks up the change on its
next replay tick.

```ts
// Backend route
app.post("/todos", async (req, res) => {
  const { id, title } = req.body;
  electrolite.execute(
    "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
    [id, currentProject(req), title],
  );
  res.json({ ok: true });
});
```

```ts
// Browser
async function addTodo(title: string) {
  await fetch("/todos", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ id: crypto.randomUUID(), title }),
  });
  // ShapeClient will see the new row on its next replay request.
}
```

Latency: write succeeds when server responds; the row appears in the
client when the next replay arrives. Typically 50–500 ms with
long-poll, faster with SSE.

## Pattern B — Optimistic state (snappy UI)

Show the write in the UI immediately, before the server confirms.
Reconcile when the next replay arrives.

```ts
import { useShape } from "electrolite-react";
import { LocalMutationBuffer } from "../../clients/browser/local-mutation-buffer.js";

const buffer = new LocalMutationBuffer<Todo>("todos:p1");

function TodosPage() {
  const { data: serverRows } = useShape<Todo>(
    "/electrolite/v1/projectTodos/p1",
  );
  // The buffer subtracts confirmed rows from its pending set as
  // they arrive in serverRows, so this view is always coherent.
  const rows = buffer.merge(serverRows);

  async function add(title: string) {
    const id = crypto.randomUUID();
    buffer.stage({ id, project_id: "p1", title, done: 0 });
    try {
      await fetch("/todos", {
        method: "POST",
        body: JSON.stringify({ id, title }),
      });
    } catch (err) {
      buffer.rollback(id);
      throw err;
    }
  }

  return <List items={rows} onAdd={add} />;
}
```

Reconciliation: when the next replay batch confirms `id`, the buffer
removes that row from its pending set. If the server-confirmed row
differs, the server wins (replays are authoritative).

## Pattern C — Persisted optimistic buffer (survives reload)

Same as Pattern B, but the buffer is persisted in `localStorage` so
optimistic state survives page reloads. On reconnect, the buffer
re-issues any unconfirmed POSTs.

```ts
const buffer = new LocalMutationBuffer<Todo>("todos:p1", {
  storage: globalThis.localStorage,
  retry: async (mutation) => {
    await fetch("/todos", {
      method: "POST",
      body: JSON.stringify(mutation),
    });
  },
});
```

The buffer's responsibilities:
- Stage local mutations and surface them in the UI immediately.
- Persist staged mutations so they survive reloads.
- On reconnect, replay unconfirmed mutations against the backend.
- When the server's replay confirms a mutation, drop it from the
  buffer.

`LocalMutationBuffer` (in `clients/browser/local-mutation-buffer.js`)
implements this for the simple "add row" case. For deeper conflict
handling — concurrent edits to the same row, server-side merges,
CRDT-like resolution — you're outside the scope of this library.
Build it on top of the buffer's hooks.

## What about offline writes?

Electrolite does not promise offline-write durability. The buffer
in Pattern C improves the experience but doesn't replace a real
offline store. If your app needs full offline-first behavior with
conflict resolution, you're better served by a CRDT layer
(Automerge, Yjs) on top of Electrolite's read sync.

## Why not bake writes into the protocol?

Two reasons:

1. **Auth is already at the shape boundary.** A write path needs its
   own auth — different rules, different validation, different audit.
   Pushing writes through the same `/electrolite/v1/...` URL would
   mix concerns and double the security surface.

2. **Apps already have write paths.** Your REST handlers, your
   GraphQL mutations, your form posts. Electrolite slots into the
   read side; the write side stays where it is.

The result: Electrolite stays small, and your app's write path stays
explicit.
