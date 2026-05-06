// Tests the cache-and-share logic underneath the React hooks.
// Full hook rendering tests would need React DOM/test-renderer; the
// cache layer below is the actual correctness surface and is what
// these tests exercise.

import assert from "node:assert/strict";
import test from "node:test";
import { getShape, getShapeStream } from "./electrolite-react.ts";

const NULL_FETCH = () => new Promise(() => {}); // never resolves

test("getShapeStream shares one ShapeClient by url+transport", () => {
  const url = "http://app.test/electrolite/v1/x/p1";
  const a = getShapeStream(url, { fetch: NULL_FETCH as any, live: false } as any);
  const b = getShapeStream(url, { fetch: NULL_FETCH as any, live: false } as any);
  assert.strictEqual(a, b);

  const c = getShapeStream(url, {
    transport: "sse",
    fetch: NULL_FETCH as any,
    live: false,
  } as any);
  assert.notStrictEqual(a, c);

  a.stop();
  c.stop();
});

test("getShape exposes a subscribable rows view that starts empty", () => {
  const url = "http://app.test/electrolite/v1/y/p1";
  const view = getShape(url, { fetch: NULL_FETCH as any, live: false } as any);
  let seen: any[] | null = null;
  const unsub = view.subscribe((rows) => {
    seen = rows;
  });
  // No fetch resolves, so rows stay [].
  assert.deepEqual(view.rows, []);
  unsub();
  // Stop the underlying client by accessing it through the stream cache.
  const client = getShapeStream(url, { fetch: NULL_FETCH as any, live: false } as any);
  client.stop();
});

test("modules export the four public hooks", async () => {
  const mod = await import("./electrolite-react.ts");
  assert.equal(typeof mod.useShape, "function");
  assert.equal(typeof mod.preloadShape, "function");
  assert.equal(typeof mod.getShapeStream, "function");
  assert.equal(typeof mod.getShape, "function");
});
