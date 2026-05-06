// Cross-language client × engine matrix.
//
// Spawns each engine as a real HTTP server and drives the same
// scenario against it from every client. New client language
// libraries are added by appending to CLIENTS.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { ShapeClient } from "../clients/browser/electrolite.js";
import { ENGINES, ensureBuilt, pickPort, sleep } from "./lib/spawn.ts";
import type { Server } from "./lib/spawn.ts";

interface ClientDef {
  name: string;
  run: (server: Server) => Promise<void>;
}

async function runBrowserClient(server: Server) {
  await runBrowserScenario(server, "long-poll");
  await runBrowserScenario(server, "sse");
}

async function runBrowserScenario(server: Server, transport: "long-poll" | "sse") {
  // Each scenario starts from an empty table, since long-poll and sse
  // share the engine's DB across the same matrix cell.
  await server.exec("DELETE FROM todos", []);

  const client = new ShapeClient(`${server.url}/electrolite/v1/projectTodos/p1`, {
    retry: { minDelayMs: 5, maxDelayMs: 20 },
    transport,
  });

  // Each transport drives the same sequence (snapshot, insert/update/
  // delete, batch, cross-shape no-op); the wait mechanism differs.
  const liveStep = transport === "long-poll"
    ? makeLongPollLive(client)
    : makeSseLive(client);

  try {
    assert.equal(await client.request({ offset: -1 }), true);
    assert.deepEqual(client.currentRows(), []);

    await liveStep.start();

    await server.exec(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, ?)",
      [1, "p1", "first", 0],
    );
    await liveStep.waitFor(() => client.currentRows().length === 1);
    assert.deepEqual(client.currentRows(), [
      { id: 1, project_id: "p1", title: "first", done: 0 },
    ]);

    await server.exec("UPDATE todos SET title = ? WHERE id = ?", ["renamed", 1]);
    await liveStep.waitFor(() => client.currentRows()[0]?.title === "renamed");

    await server.exec("DELETE FROM todos WHERE id = ?", [1]);
    await liveStep.waitFor(() => client.currentRows().length === 0);

    await server.writeBatch([
      ["INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [10, "p1", "a"]],
      ["INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)", [11, "p1", "b"]],
    ]);
    await liveStep.waitFor(
      () => client.currentRows().map((r: any) => r.id).join(",") === "10,11",
    );

    // Cross-shape: write that doesn't match our predicate must never
    // surface in materialized rows.
    await server.exec(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
      [99, "p2", "ignore me"],
    );
    await sleep(150);
    assert.equal(
      client.currentRows().some((r: any) => r.id === 99),
      false,
    );

    await liveStep.stop();
  } finally {
    client.stop();
  }
}

interface LiveStep {
  start: () => Promise<void>;
  waitFor: (predicate: () => boolean) => Promise<void>;
  stop: () => Promise<void>;
}

function makeLongPollLive(client: any): LiveStep {
  let pending: Promise<unknown> | null = null;
  const ensurePending = () => {
    if (!pending) {
      pending = client.request({ offset: client.offset, live: true });
    }
  };
  return {
    async start() {
      ensurePending();
      await sleep(25);
    },
    async waitFor(predicate) {
      const deadline = Date.now() + 3_000;
      while (Date.now() < deadline) {
        if (predicate()) return;
        if (pending) {
          await pending;
          pending = null;
        }
        if (predicate()) return;
        ensurePending();
        await sleep(15);
      }
      throw new Error("long-poll waitFor timed out");
    },
    async stop() {
      if (pending) {
        // best-effort: client.stop() will abort the in-flight request
      }
    },
  };
}

function makeSseLive(client: any): LiveStep {
  let task: Promise<unknown> | null = null;
  return {
    async start() {
      task = client.streamSse().catch(() => {});
      // Give the connection a beat to establish.
      await sleep(50);
    },
    async waitFor(predicate) {
      const deadline = Date.now() + 3_000;
      while (Date.now() < deadline) {
        if (predicate()) return;
        await sleep(15);
      }
      throw new Error("sse waitFor timed out");
    },
    async stop() {
      // Abort the in-flight fetch so the SSE task resolves, then
      // await it so node:test doesn't flag asynchronous activity
      // past test end.
      client.stop();
      if (task) await task;
    },
  };
}

const CLIENTS: ClientDef[] = [{ name: "browser", run: runBrowserClient }];

ensureBuilt();

for (const engine of ENGINES) {
  for (const client of CLIENTS) {
    test(`matrix: ${client.name} client ↔ ${engine.name} engine`, async () => {
      const dir = mkdtempSync(join(tmpdir(), `electrolite-matrix-${engine.name}-`));
      const port = pickPort();
      const dbPath = join(dir, "app.db");
      let server: Server | null = null;
      try {
        server = await engine.start(port, dbPath);
        await client.run(server);
      } finally {
        if (server) await server.stop();
        rmSync(dir, { recursive: true, force: true });
      }
    });
  }
}
