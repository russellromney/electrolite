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
