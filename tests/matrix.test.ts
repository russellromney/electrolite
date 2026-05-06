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
  const client = new ShapeClient(`${server.url}/electrolite/v1/projectTodos/p1`, {
    retry: { minDelayMs: 5, maxDelayMs: 20 },
  });

  try {
    assert.equal(await client.request({ offset: -1 }), true);
    assert.deepEqual(client.currentRows(), []);

    let live = client.request({ offset: client.offset, live: true });
    await sleep(25);
    await server.exec(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, ?)",
      [1, "p1", "first", 0],
    );
    assert.equal(await live, true);
    assert.deepEqual(client.currentRows(), [
      { id: 1, project_id: "p1", title: "first", done: 0 },
    ]);

    live = client.request({ offset: client.offset, live: true });
    await sleep(25);
    await server.exec("UPDATE todos SET title = ? WHERE id = ?", ["renamed", 1]);
    assert.equal(await live, true);
    assert.equal(client.currentRows()[0].title, "renamed");

    live = client.request({ offset: client.offset, live: true });
    await sleep(25);
    await server.exec("DELETE FROM todos WHERE id = ?", [1]);
    assert.equal(await live, true);
    assert.deepEqual(client.currentRows(), []);

    live = client.request({ offset: client.offset, live: true });
    await sleep(25);
    await server.writeBatch([
      [
        "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
        [10, "p1", "a"],
      ],
      [
        "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
        [11, "p1", "b"],
      ],
    ]);
    assert.equal(await live, true);
    assert.deepEqual(
      client.currentRows().map((r: any) => r.id),
      [10, 11],
    );

    live = client.request({ offset: client.offset, live: true });
    await sleep(25);
    await server.exec(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
      [99, "p2", "ignore me"],
    );
    await live;
    assert.equal(
      client.currentRows().some((r: any) => r.id === 99),
      false,
    );
  } finally {
    client.stop();
  }
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
