import { mkdtempSync, rmSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ShapeClient } from "../../clients/browser/electrolite.js";
import {
  createElectrolite,
  eq,
  shape,
} from "../../packages/electrolite-node/electrolite-node.ts";

const clientCount = Number(process.env.ELECTROLITE_FANOUT_CLIENTS ?? 100);
const dir = mkdtempSync(join(tmpdir(), "electrolite-fanout-"));
const dbPath = join(dir, "fanout.db");

const electrolite = createElectrolite({
  dbPath,
  liveTimeoutMs: 10_000,
  pollIntervalMs: 1_000,
  shapes: {
    projectTodos: shape({
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      params: ["projectId"],
      where: ({ params }) => eq("project_id", params.projectId),
      scope: ({ params }) => `project:${params.projectId}`,
      authorize: ({ params, context }) => context.projectIds.has(params.projectId),
    }),
  },
});

try {
  electrolite.executeBatch(`
    CREATE TABLE todos (
      id INTEGER PRIMARY KEY,
      project_id TEXT NOT NULL,
      title TEXT NOT NULL,
      done BOOLEAN NOT NULL DEFAULT 0
    );
  `);
  electrolite.installTriggers("todos");
  electrolite.execute(
    "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
    [1, "launch", "Initial todo"],
  );

  const fetch = (url, init) => {
    return electrolite.handle(new Request(url, init), {
      projectIds: new Set(["launch"]),
    });
  };
  const clients = Array.from({ length: clientCount }, () => new ShapeClient(
    "https://app.example/electrolite/v1/projectTodos/launch",
    { fetch, live: false },
  ));

  const snapshotStart = performance.now();
  await Promise.all(clients.map((client) => client.request({ offset: -1 })));
  const snapshotMs = performance.now() - snapshotStart;
  const offset = clients[0].offset;

  const liveRequests = clients.map((client) => client.request({
    offset: client.offset,
    live: true,
  }));

  await new Promise((resolve) => setTimeout(resolve, 25));
  const fanoutStart = performance.now();
  electrolite.execute(
    "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
    [2, "launch", "Wake 100 subscribers"],
  );
  const results = await Promise.all(liveRequests);
  const fanoutMs = performance.now() - fanoutStart;
  const woke = results.filter(Boolean).length;
  const rowsVisible = clients.filter((client) => client.currentRows().length === 2).length;

  console.log(`Clients: ${clientCount}`);
  console.log(`Initial snapshot offset: ${offset}`);
  console.log(`Snapshot all clients: ${snapshotMs.toFixed(1)}ms`);
  console.log(`Woke clients: ${woke}/${clientCount}`);
  console.log(`Clients with new row: ${rowsVisible}/${clientCount}`);
  console.log(`Fanout after one SQLite write: ${fanoutMs.toFixed(1)}ms`);
} finally {
  rmSync(dir, { recursive: true, force: true });
}
