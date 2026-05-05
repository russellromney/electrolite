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
      rows: [
        { id: 1, name: "Ada", active: 1 },
      ],
      offset: 2,
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
