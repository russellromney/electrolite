import assert from "node:assert/strict";
import test from "node:test";
import { ShapeClient } from "./electrolite.js";

test("materializes snapshots and replay messages", () => {
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async () => {
      throw new Error("unused");
    },
  });
  const seen = [];
  client.subscribe((rows) => seen.push(rows));

  assert.equal(
    client.apply({
      type: "snapshot",
      key_columns: ["id"],
      rows: [
        { id: 1, name: "Ada", active: 1 },
      ],
      offset: 2,
      up_to_date: true,
    }),
    true,
  );
  assert.deepEqual(client.currentRows(), [{ id: 1, name: "Ada", active: 1 }]);
  assert.equal(client.offset, 2);

  assert.equal(
    client.apply({
      type: "replay",
      messages: [
        {
          type: "insert",
          key: { id: 2 },
          value: { id: 2, name: "Grace", active: 1 },
          offset: 3,
        },
        {
          type: "update",
          key: { id: 1 },
          value: { id: 1, name: "Ada Lovelace", active: 1 },
          offset: 4,
        },
      ],
      offset: 4,
      up_to_date: true,
    }),
    true,
  );
  assert.deepEqual(client.currentRows(), [
    { id: 1, name: "Ada Lovelace", active: 1 },
    { id: 2, name: "Grace", active: 1 },
  ]);

  assert.equal(
    client.apply({
      type: "replay",
      messages: [
        {
          type: "delete",
          key: { id: 1 },
          offset: 5,
        },
      ],
      offset: 5,
      up_to_date: true,
    }),
    true,
  );
  assert.deepEqual(client.currentRows(), [{ id: 2, name: "Grace", active: 1 }]);
  assert.equal(client.offset, 5);
  assert.equal(seen.length, 4);
});

test("treats 204 live timeouts as no change", async () => {
  const requested = [];
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async (url) => {
      requested.push(String(url));
      return { status: 204, ok: true };
    },
  });

  assert.equal(await client.request({ offset: 10, live: true }), false);
  assert.equal(
    requested[0],
    "http://app.test/electrolite/v1/shape/activeUsers?offset=10&live=true",
  );
});

test("handles resync_required by clearing rows and fetching a new snapshot", async () => {
  const responses = [
    { status: 409, ok: false },
    {
      status: 200,
      ok: true,
      json: async () => ({
        type: "snapshot",
        key_columns: ["id"],
        rows: [{ id: 7, name: "Ada", active: 1 }],
        offset: 42,
        up_to_date: true,
      }),
    },
  ];
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async () => responses.shift(),
  });
  client.apply({
    type: "snapshot",
    key_columns: ["id"],
    rows: [{ id: 1, name: "Old", active: 1 }],
    offset: 10,
    up_to_date: true,
  });

  assert.equal(await client.request({ offset: 10, live: true }), true);
  assert.deepEqual(client.currentRows(), [{ id: 7, name: "Ada", active: 1 }]);
  assert.equal(client.offset, 42);
});

test("emits status updates", async () => {
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async () => ({
      status: 200,
      ok: true,
      json: async () => ({
        type: "snapshot",
        key_columns: ["id"],
        rows: [],
        offset: 0,
        up_to_date: true,
      }),
    }),
  });
  const statuses = [];
  client.subscribeStatus((status) => statuses.push(status.type));

  await client.request({ offset: -1 });

  assert.deepEqual(statuses, ["idle", "snapshot", "ready"]);
});

test("infers key columns from snapshots", () => {
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    fetch: async () => {
      throw new Error("unused");
    },
  });

  client.apply({
    type: "snapshot",
    key_columns: ["id"],
    rows: [{ id: 1, name: "Ada", active: 1 }],
    offset: 2,
    up_to_date: true,
  });

  assert.deepEqual(client.currentRows(), [{ id: 1, name: "Ada", active: 1 }]);
  assert.deepEqual(client.keyColumns, ["id"]);
});

test("stages replay messages until up_to_date", () => {
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async () => {
      throw new Error("unused");
    },
  });
  const seen = [];
  client.subscribe((rows) => seen.push(rows));
  client.apply({
    type: "snapshot",
    key_columns: ["id"],
    rows: [{ id: 1, name: "Ada", active: 1 }],
    offset: 1,
    up_to_date: true,
  });

  assert.equal(
    client.apply({
      type: "replay",
      messages: [
        {
          type: "update",
          key: { id: 1 },
          value: { id: 1, name: "Ada Lovelace", active: 1 },
          offset: 2,
        },
      ],
      offset: 2,
      up_to_date: false,
    }),
    false,
  );
  assert.deepEqual(client.currentRows(), [{ id: 1, name: "Ada", active: 1 }]);

  assert.equal(
    client.apply({
      type: "replay",
      messages: [],
      offset: 2,
      up_to_date: true,
    }),
    true,
  );
  assert.deepEqual(client.currentRows(), [
    { id: 1, name: "Ada Lovelace", active: 1 },
  ]);
  assert.equal(seen.length, 3);
});

test("emits low-level snapshot, replay, message, and offset events", () => {
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async () => {
      throw new Error("unused");
    },
  });
  const events = [];
  client.subscribeEvents((event) => events.push(event.type));

  client.apply({
    type: "snapshot",
    key_columns: ["id"],
    rows: [{ id: 1, name: "Ada", active: 1 }],
    offset: 1,
    up_to_date: true,
  });
  client.apply({
    type: "replay",
    messages: [
      {
        type: "update",
        key: { id: 1 },
        value: { id: 1, name: "Ada Lovelace", active: 1 },
        offset: 2,
      },
    ],
    offset: 2,
    up_to_date: true,
  });

  assert.deepEqual(events, ["snapshot", "offset", "update", "replay", "offset"]);
});

test("hydrates from and persists to a storage adapter", async () => {
  let saved = {
    keyColumns: ["id"],
    logId: "log-a",
    shapeHandle: "shape-a",
    offset: 9,
    rows: [{ id: 1, name: "Cached", active: 1 }],
  };
  const storage = {
    load: async () => saved,
    save: async (state) => {
      saved = state;
    },
    clear: async () => {
      saved = null;
    },
  };
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    persist: storage,
    fetch: async () => ({ status: 204, ok: true }),
  });

  assert.equal(await client.hydrate(), true);
  assert.equal(client.logId, "log-a");
  assert.equal(client.shapeHandle, "shape-a");
  assert.equal(client.offset, 9);
  assert.deepEqual(client.currentRows(), []);

  client.apply({
    type: "replay",
    messages: [
      {
        type: "insert",
        key: { id: 2 },
        value: { id: 2, name: "Grace", active: 1 },
        offset: 10,
      },
    ],
    offset: 10,
    up_to_date: true,
  });
  await Promise.resolve();

  assert.equal(saved.offset, 10);
  assert.equal(saved.logId, "log-a");
  assert.equal(saved.shapeHandle, "shape-a");
  assert.deepEqual(saved.rows, [
    { id: 1, name: "Cached", active: 1 },
    { id: 2, name: "Grace", active: 1 },
  ]);
});

test("does not publish persisted rows until the shape cache is validated", async () => {
  const storage = {
    load: async () => ({
      keyColumns: ["id"],
      logId: "log-a",
      shapeHandle: "shape-a",
      offset: 9,
      rows: [{ id: 1, name: "Cached", active: 1 }],
    }),
    save: async () => {},
    clear: async () => {},
  };
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    persist: storage,
    fetch: async () => {
      throw new Error("unused");
    },
  });
  const seen = [];
  client.subscribe((rows) => seen.push(rows));

  assert.equal(await client.hydrate(), true);
  assert.deepEqual(seen, [[]]);
  assert.deepEqual(client.currentRows(), []);

  client.apply({
    type: "replay",
    log_id: "log-a",
    shape_handle: "shape-a",
    messages: [],
    offset: 9,
    up_to_date: true,
  });

  assert.deepEqual(client.currentRows(), [{ id: 1, name: "Cached", active: 1 }]);
});

test("clears persisted rows that predate shape_handle support", async () => {
  let cleared = false;
  const storage = {
    load: async () => ({
      keyColumns: ["id"],
      logId: "log-a",
      offset: 9,
      rows: [{ id: 1, name: "Old", active: 1 }],
    }),
    save: async () => {},
    clear: async () => {
      cleared = true;
    },
  };
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    persist: storage,
    fetch: async () => {
      throw new Error("unused");
    },
  });

  assert.equal(await client.hydrate(), false);
  assert.equal(cleared, true);
  assert.equal(client.offset, -1);
  assert.deepEqual(client.currentRows(), []);
});

test("requires a fresh snapshot when the persisted shape_handle no longer matches", () => {
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async () => {
      throw new Error("unused");
    },
  });
  client.apply({
    type: "snapshot",
    log_id: "log-a",
    shape_handle: "shape-a",
    key_columns: ["id"],
    rows: [{ id: 1, name: "Ada", active: 1 }],
    offset: 2,
    up_to_date: true,
  });

  assert.equal(
    client.apply({
      type: "replay",
      log_id: "log-a",
      shape_handle: "shape-b",
      messages: [
        {
          type: "insert",
          key: { id: 2 },
          value: { id: 2, name: "Grace", active: 1 },
          offset: 3,
        },
      ],
      offset: 3,
      up_to_date: true,
    }),
    false,
  );
  assert.equal(client.offset, -1);
  assert.deepEqual(client.currentRows(), []);
});

test("awaits cache clearing before fetching a resync snapshot from request", async () => {
  const calls = [];
  const storage = {
    load: async () => null,
    save: async (state) => calls.push(["save", state.offset]),
    clear: async () => {
      await Promise.resolve();
      calls.push(["clear"]);
    },
  };
  const responses = [
    {
      status: 200,
      ok: true,
      json: async () => ({
        type: "replay",
        log_id: "log-a",
        shape_handle: "shape-b",
        messages: [],
        offset: 3,
        up_to_date: true,
      }),
    },
    {
      status: 200,
      ok: true,
      json: async () => ({
        type: "snapshot",
        log_id: "log-a",
        shape_handle: "shape-b",
        key_columns: ["id"],
        rows: [{ id: 2, name: "Grace", active: 1 }],
        offset: 3,
        up_to_date: true,
      }),
    },
  ];
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    persist: storage,
    keyColumns: ["id"],
    fetch: async () => responses.shift(),
  });
  client.logId = "log-a";
  client.shapeHandle = "shape-a";
  client.offset = 2;

  assert.equal(await client.request({ offset: 2 }), true);
  assert.deepEqual(calls, [["clear"], ["save", 3]]);
  assert.deepEqual(client.currentRows(), [{ id: 2, name: "Grace", active: 1 }]);
});

test("sends cached log_id with replay and live requests", async () => {
  const requested = [];
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    fetch: async (url) => {
      requested.push(String(url));
      return { status: 204, ok: true };
    },
  });
  client.logId = "abc123";

  assert.equal(await client.request({ offset: 10, live: true }), false);
  assert.equal(
    requested[0],
    "http://app.test/electrolite/v1/shape/activeUsers?offset=10&live=true&log_id=abc123",
  );
});

test("drains replay pages before switching back to live requests", async () => {
  const requested = [];
  const responses = [
    {
      type: "snapshot",
      log_id: "log-a",
      shape_handle: "shape-a",
      key_columns: ["id"],
      rows: [],
      offset: 1,
      up_to_date: true,
    },
    {
      type: "replay",
      log_id: "log-a",
      shape_handle: "shape-a",
      messages: [
        {
          type: "insert",
          key: { id: 1 },
          value: { id: 1, name: "Ada", active: 1 },
          offset: 2,
        },
      ],
      offset: 2,
      up_to_date: false,
    },
    {
      type: "replay",
      log_id: "log-a",
      shape_handle: "shape-a",
      messages: [],
      offset: 2,
      up_to_date: true,
    },
    null,
  ];
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    fetch: async (url) => {
      requested.push(String(url));
      const body = responses.shift();
      if (!body) {
        client.stop();
        return { status: 204, ok: true };
      }
      return { status: 200, ok: true, json: async () => body };
    },
  });

  await client.start();

  assert.deepEqual(requested, [
    "http://app.test/electrolite/v1/shape/activeUsers?offset=-1",
    "http://app.test/electrolite/v1/shape/activeUsers?offset=1&live=true&log_id=log-a&shape_handle=shape-a",
    "http://app.test/electrolite/v1/shape/activeUsers?offset=2&log_id=log-a&shape_handle=shape-a",
    "http://app.test/electrolite/v1/shape/activeUsers?offset=2&live=true&log_id=log-a&shape_handle=shape-a",
  ]);
});

test("ignores persisted offsets that predate log_id support", async () => {
  let cleared = false;
  const storage = {
    load: async () => ({
      keyColumns: ["id"],
      offset: 9,
      rows: [{ id: 1, name: "Old", active: 1 }],
    }),
    save: async () => {},
    clear: async () => {
      cleared = true;
    },
  };
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    persist: storage,
    fetch: async () => {
      throw new Error("unused");
    },
  });

  assert.equal(await client.hydrate(), false);
  assert.equal(cleared, true);
  assert.equal(client.offset, -1);
  assert.deepEqual(client.currentRows(), []);
});

test("broadcasts applied state to another tab client", () => {
  const bus = new TestChannelBus();
  const leader = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    multiTab: true,
    channelFactory: (name) => bus.channel(name),
    fetch: async () => {
      throw new Error("unused");
    },
  });
  const follower = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    multiTab: true,
    channelFactory: (name) => bus.channel(name),
    fetch: async () => {
      throw new Error("unused");
    },
  });

  leader.apply({
    type: "snapshot",
    key_columns: ["id"],
    rows: [{ id: 1, name: "Ada", active: 1 }],
    offset: 2,
    up_to_date: true,
  });

  assert.deepEqual(follower.currentRows(), [{ id: 1, name: "Ada", active: 1 }]);
  assert.equal(follower.offset, 2);
  leader.stop();
  follower.stop();
});

test("multi-tab clients release leadership on pagehide", () => {
  const previousLocalStorage = globalThis.localStorage;
  const previousAddEventListener = globalThis.addEventListener;
  const previousRemoveEventListener = globalThis.removeEventListener;
  const storage = new Map();
  const listeners = new Map();
  globalThis.localStorage = {
    getItem: (key) => storage.get(key) ?? null,
    setItem: (key, value) => storage.set(key, value),
    removeItem: (key) => storage.delete(key),
  };
  globalThis.addEventListener = (type, listener) => listeners.set(type, listener);
  globalThis.removeEventListener = (type, listener) => {
    if (listeners.get(type) === listener) {
      listeners.delete(type);
    }
  };

  const client = new ShapeClient("http://app.test/electrolite/v1/shape/activeUsers", {
    keyColumns: ["id"],
    multiTab: true,
    // Force the localStorage lease fallback (this test covers that path;
    // the Web Locks path is covered separately).
    locks: null,
    channelFactory: () => ({
      addEventListener() {},
      removeEventListener() {},
      close() {},
      postMessage() {},
    }),
    fetch: async () => {
      throw new Error("unused");
    },
  });
  try {
    assert.equal(client.canUseNetwork(), true);
    assert.equal(storage.size, 1);
    listeners.get("pagehide")();
    assert.equal(storage.size, 0);
  } finally {
    client.stop();
    if (previousLocalStorage === undefined) {
      delete globalThis.localStorage;
    } else {
      globalThis.localStorage = previousLocalStorage;
    }
    if (previousAddEventListener === undefined) {
      delete globalThis.addEventListener;
    } else {
      globalThis.addEventListener = previousAddEventListener;
    }
    if (previousRemoveEventListener === undefined) {
      delete globalThis.removeEventListener;
    } else {
      globalThis.removeEventListener = previousRemoveEventListener;
    }
  }
});

test("calls headers callback on every request", async () => {
  const seenHeaders: any[] = [];
  let token = "tok-1";
  const client = new ShapeClient("http://app.test/electrolite/v1/x/p1", {
    keyColumns: ["id"],
    headers: () => ({ Authorization: `Bearer ${token}` }),
    fetch: async (_url, init) => {
      seenHeaders.push(init?.headers);
      return {
        ok: true,
        status: 200,
        json: async () => ({
          type: "snapshot",
          key_columns: ["id"],
          rows: [],
          offset: 0,
          up_to_date: true,
          log_id: "x",
          shape_handle: "y",
        }),
      };
    },
  });

  await client.request({ offset: -1 });
  assert.deepEqual(seenHeaders[0], { Authorization: "Bearer tok-1" });

  token = "tok-2";
  await client.request({ offset: client.offset });
  assert.deepEqual(seenHeaders[1], { Authorization: "Bearer tok-2" });
});

test("onError callback can recover from a 401 by returning new headers", async () => {
  let calls = 0;
  let token = "expired";
  const client = new ShapeClient("http://app.test/electrolite/v1/x/p1", {
    keyColumns: ["id"],
    headers: () => ({ Authorization: `Bearer ${token}` }),
    fetch: async (_url, init) => {
      calls += 1;
      const auth = (init?.headers as any)?.Authorization;
      if (auth === "Bearer expired") {
        return { ok: false, status: 401, json: async () => ({ error: "expired" }) };
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({
          type: "snapshot",
          key_columns: ["id"],
          rows: [],
          offset: 0,
          up_to_date: true,
          log_id: "x",
          shape_handle: "y",
        }),
      };
    },
    onError: async (err: any) => {
      if (err.status === 401) {
        token = "fresh";
        return {};
      }
      return undefined;
    },
  });

  await client.request({ offset: -1 });
  // Two calls: first returns 401, second succeeds with refreshed token.
  assert.equal(calls, 2);
});

test("onError retry is capped so a misbehaving callback can't loop", async () => {
  let calls = 0;
  const client = new ShapeClient("http://app.test/electrolite/v1/x/p1", {
    keyColumns: ["id"],
    fetch: async () => {
      calls += 1;
      return { ok: false, status: 500, json: async () => ({ error: "internal" }) };
    },
    onError: async () => ({}), // always tries to retry
  });

  await assert.rejects(() => client.request({ offset: -1 }));
  // Initial attempt + 3 capped retries = 4 calls.
  assert.equal(calls, 4);
});

test("SSE onError can recover from a 401 by returning new headers", async () => {
  let calls = 0;
  let token = "expired";
  const client = new ShapeClient("http://app.test/electrolite/v1/x/p1", {
    keyColumns: ["id"],
    transport: "sse",
    headers: () => ({ Authorization: `Bearer ${token}` }),
    fetch: async (_url, init) => {
      calls += 1;
      const auth = (init?.headers as any)?.Authorization;
      if (auth === "Bearer expired") {
        return { ok: false, status: 401, body: null };
      }
      // Successful SSE response — return a body with one snapshot
      // event then immediately close, so streamSse exits cleanly.
      const encoder = new TextEncoder();
      const frame =
        "event: snapshot\ndata: " +
        JSON.stringify({
          type: "snapshot",
          key_columns: ["id"],
          rows: [],
          offset: 0,
          up_to_date: true,
          log_id: "x",
          shape_handle: "y",
        }) +
        "\n\n";
      const body = {
        getReader() {
          let sent = false;
          return {
            async read() {
              if (sent) return { done: true, value: undefined };
              sent = true;
              return { done: false, value: encoder.encode(frame) };
            },
            async cancel() {},
          };
        },
      };
      return { ok: true, status: 200, body };
    },
    onError: async (err: any) => {
      if (err.status === 401) {
        token = "fresh";
        return {};
      }
      return undefined;
    },
  });

  await client.streamSse();
  // Two calls: initial 401, retry succeeds with refreshed token.
  assert.equal(calls, 2);
});

test("SSE onError retry is capped so a misbehaving callback can't loop", async () => {
  let calls = 0;
  const client = new ShapeClient("http://app.test/electrolite/v1/x/p1", {
    keyColumns: ["id"],
    transport: "sse",
    fetch: async () => {
      calls += 1;
      return { ok: false, status: 500, body: null };
    },
    onError: async () => ({}), // always tries to retry
  });

  await assert.rejects(() => client.streamSse());
  // Initial attempt + 3 capped retries = 4 calls.
  assert.equal(calls, 4);
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function stubChannel() {
  return {
    addEventListener() {},
    removeEventListener() {},
    close() {},
    postMessage() {},
  };
}

// --- F6: multi-tab leadership must grant to exactly one tab. The old
// localStorage check-then-act let two tabs both win. Web Locks give
// true mutual exclusion. ---
test("multi-tab leadership grants to exactly one client via Web Locks (F6)", async () => {
  const url = "http://app.test/electrolite/v1/shape/f6-locktest";
  const opts = {
    keyColumns: ["id"],
    multiTab: true,
    channelFactory: () => stubChannel(),
    fetch: async () => {
      throw new Error("unused");
    },
  };
  const a = new ShapeClient(url, opts);
  const b = new ShapeClient(url, opts);
  try {
    // Both tabs are backed by the platform Web Locks API (Node ships one).
    assert.ok(a.locks && b.locks);

    // Poll like the real start() loop: keep asking until leadership
    // settles. At most one tab may ever hold it.
    const pollUntil = async (cond, ...clients) => {
      const deadline = Date.now() + 500;
      while (Date.now() < deadline && !cond()) {
        for (const c of clients) c.canUseNetwork();
        await sleep(10);
      }
    };
    await pollUntil(() => a.leaderHeld || b.leaderHeld, a, b);
    assert.equal(
      [a.leaderHeld, b.leaderHeld].filter(Boolean).length,
      1,
      "exactly one tab may hold leadership",
    );

    // When the leader steps down, the follower can take over.
    const leader = a.leaderHeld ? a : b;
    const follower = a.leaderHeld ? b : a;
    leader.releaseLeadership();
    await pollUntil(() => follower.leaderHeld, follower);
    assert.equal(follower.leaderHeld, true);
    assert.equal(leader.leaderHeld, false);
  } finally {
    a.stop();
    b.stop();
  }
});

// --- F7: a replica=diff UPDATE for a key we don't hold can't be merged.
// The client must resync rather than write the sparse value as a whole
// (partial) row. ---
test("diff update for an unknown key forces resync, never a partial row (F7)", () => {
  const client = new ShapeClient("http://app.test/electrolite/v1/shape/f7", {
    keyColumns: ["id"],
    replica: "diff",
  });
  client.apply({
    type: "snapshot",
    log_id: "l",
    shape_handle: "s",
    key_columns: ["id"],
    rows: [{ id: 1, name: "a", active: 1 }],
    offset: 1,
    up_to_date: true,
  });
  const changed = client.apply({
    type: "replay",
    log_id: "l",
    shape_handle: "s",
    replica: "diff",
    messages: [{ type: "update", key: { id: 2 }, value: { active: 0 }, offset: 2 }],
    offset: 2,
    up_to_date: true,
  });
  assert.equal(changed, false);
  assert.equal(client.resyncRequired, true);
  // No partial row for the unknown key was materialized.
  assert.equal(client.rows.has(JSON.stringify({ id: 2 })), false);
});

// --- F7 over SSE: a diff UPDATE for an unknown key, delivered as an SSE
// frame, must break the stale stream and resync (not keep streaming
// against dropped/partial state). ---
test("SSE resyncs when a diff update targets an unknown key (F7 over SSE)", async () => {
  const encoder = new TextEncoder();
  const frames = [
    "event: snapshot\ndata: " +
      JSON.stringify({
        type: "snapshot",
        key_columns: ["id"],
        rows: [{ id: 1, name: "a", active: 1 }],
        offset: 1,
        up_to_date: true,
        log_id: "l",
        shape_handle: "s",
      }) +
      "\n\n",
    "event: replay\ndata: " +
      JSON.stringify({
        type: "replay",
        log_id: "l",
        shape_handle: "s",
        replica: "diff",
        messages: [{ type: "update", key: { id: 2 }, value: { active: 0 }, offset: 2 }],
        offset: 2,
        up_to_date: true,
      }) +
      "\n\n",
  ];
  const requests = [];
  const client = new ShapeClient("http://app.test/electrolite/v1/x/p1", {
    keyColumns: ["id"],
    transport: "sse",
    replica: "diff",
    fetch: async (url) => {
      requests.push(String(url));
      if (requests.length === 1) {
        const body = {
          getReader() {
            let i = 0;
            return {
              async read() {
                if (i >= frames.length) return { done: true, value: undefined };
                return { done: false, value: encoder.encode(frames[i++]) };
              },
              async cancel() {},
            };
          },
        };
        return { ok: true, status: 200, body };
      }
      // Recovery snapshot after the unmergeable diff forced a resync.
      return {
        ok: true,
        status: 200,
        json: async () => ({
          type: "snapshot",
          key_columns: ["id"],
          rows: [
            { id: 1, name: "a", active: 1 },
            { id: 2, name: "b", active: 0 },
          ],
          offset: 3,
          up_to_date: true,
          log_id: "l",
          shape_handle: "s",
        }),
      };
    },
  });

  await client.streamSse();

  assert.equal(requests.length, 2, "an unmergeable diff frame must trigger a recovery snapshot");
  assert.ok(requests[1].includes("offset=-1"));
  // After recovery the unknown key is a full row, not the sparse diff.
  const row2 = client.rows.get(JSON.stringify({ id: 2 }));
  assert.ok(row2);
  assert.equal(row2.name, "b");
  client.stop();
});

class TestChannelBus {
  constructor() {
    this.channels = new Map();
  }

  channel(name) {
    const bus = this;
    const channel = {
      name,
      onmessage: null,
      listeners: new Set(),
      addEventListener(type, listener) {
        if (type === "message") {
          this.listeners.add(listener);
        }
      },
      removeEventListener(type, listener) {
        if (type === "message") {
          this.listeners.delete(listener);
        }
      },
      postMessage(message) {
        for (const peer of bus.channels.get(name) ?? []) {
          if (peer === this) {
            continue;
          }
          const event = { data: message };
          peer.onmessage?.(event);
          for (const listener of peer.listeners) {
            listener(event);
          }
        }
      },
      close() {
        bus.channels.get(name)?.delete(this);
      },
    };
    if (!this.channels.has(name)) {
      this.channels.set(name, new Set());
    }
    this.channels.get(name).add(channel);
    return channel;
  }
}
