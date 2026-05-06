// Tests the cache-and-share logic underneath the React hooks.
// Full hook rendering tests would need React DOM/test-renderer; the
// cache layer below is the actual correctness surface and is what
// these tests exercise.

import assert from "node:assert/strict";
import test from "node:test";
import { getShape, getShapeStream } from "./electrolite-react.ts";

const NULL_FETCH = () => new Promise(() => {}); // never resolves

test("getShapeStream shares one ShapeClient by url+transport, dispose returns", () => {
  const url = "http://app.test/electrolite/v1/x/p1";
  const a = getShapeStream(url, { fetch: NULL_FETCH as any, live: false } as any);
  const b = getShapeStream(url, { fetch: NULL_FETCH as any, live: false } as any);
  // Same underlying client.
  assert.strictEqual(a.client, b.client);

  const c = getShapeStream(url, {
    transport: "sse",
    fetch: NULL_FETCH as any,
    live: false,
  } as any);
  // Different transport → different client.
  assert.notStrictEqual(a.client, c.client);

  a.dispose();
  b.dispose();
  c.dispose();
});

test("explicit cacheKey scopes entries (auth-leak fix)", () => {
  const url = "http://app.test/electrolite/v1/multi-user/p1";
  const userA = getShapeStream(url, {
    fetch: NULL_FETCH as any,
    live: false,
    cacheKey: `userA::${url}`,
  } as any);
  const userB = getShapeStream(url, {
    fetch: NULL_FETCH as any,
    live: false,
    cacheKey: `userB::${url}`,
  } as any);
  // Different cacheKey → different client; one user's headers don't
  // leak to another.
  assert.notStrictEqual(userA.client, userB.client);
  userA.dispose();
  userB.dispose();
});

test("getShape exposes a subscribable rows view; dispose releases", () => {
  const url = "http://app.test/electrolite/v1/y/p1";
  const view = getShape(url, { fetch: NULL_FETCH as any, live: false } as any);
  let seen: any[] | null = null;
  const unsub = view.subscribe((rows) => {
    seen = rows;
  });
  // No fetch resolves, so rows stay [].
  assert.deepEqual(view.rows, []);
  unsub();
  view.dispose();
});

test("getShape dispose is idempotent", () => {
  const url = "http://app.test/electrolite/v1/idempotent/p1";
  const view = getShape(url, { fetch: NULL_FETCH as any, live: false } as any);
  view.dispose();
  // Second dispose must be a no-op (no double-stop, no throw).
  view.dispose();
});

test("modules export the four public hooks", async () => {
  const mod = await import("./electrolite-react.ts");
  assert.equal(typeof mod.useShape, "function");
  assert.equal(typeof mod.preloadShape, "function");
  assert.equal(typeof mod.getShapeStream, "function");
  assert.equal(typeof mod.getShape, "function");
});
