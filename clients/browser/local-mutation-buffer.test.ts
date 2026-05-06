import assert from "node:assert/strict";
import test from "node:test";
import { LocalMutationBuffer } from "./local-mutation-buffer.js";

test("staged rows show up in merged result, server-confirmed rows replace them", () => {
  const buffer = new LocalMutationBuffer("test");
  buffer.stage({ id: "a", title: "first" });
  buffer.stage({ id: "b", title: "second" });

  // No server rows yet — both pending appear.
  assert.deepEqual(buffer.merge([]), [
    { id: "a", title: "first" },
    { id: "b", title: "second" },
  ]);

  // Server confirms `a` (replay just delivered it). Buffer drops it.
  const merged = buffer.merge([{ id: "a", title: "first" }]);
  assert.deepEqual(merged.map((r: any) => r.id).sort(), ["a", "b"]);
  // After merge, only `b` remains pending.
  buffer.rollback("a"); // already dropped — no-op
  assert.deepEqual(
    buffer.merge([{ id: "a", title: "first" }, { id: "b", title: "second" }]),
    [{ id: "a", title: "first" }, { id: "b", title: "second" }],
  );
});

test("rollback removes a staged row", () => {
  const buffer = new LocalMutationBuffer("test2");
  buffer.stage({ id: "x", title: "stale" });
  assert.equal(buffer.merge([]).length, 1);
  buffer.rollback("x");
  assert.deepEqual(buffer.merge([]), []);
});

test("persists to storage when one is provided", () => {
  const store = new Map<string, string>();
  const fakeStorage = {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => store.set(k, v),
    removeItem: (k: string) => store.delete(k),
  };
  const buffer = new LocalMutationBuffer("persistent", { storage: fakeStorage });
  buffer.stage({ id: "a", title: "first" });

  // New buffer with the same storage should hydrate the pending set.
  const buffer2 = new LocalMutationBuffer("persistent", { storage: fakeStorage });
  assert.deepEqual(buffer2.merge([]), [{ id: "a", title: "first" }]);
});

test("replay calls the retry function for each pending mutation", async () => {
  const seen: any[] = [];
  const buffer = new LocalMutationBuffer("retrying", {
    retry: async (row: any) => {
      seen.push(row);
    },
  });
  buffer.stage({ id: "1", title: "a" });
  buffer.stage({ id: "2", title: "b" });
  await buffer.replay();
  assert.equal(seen.length, 2);
});
