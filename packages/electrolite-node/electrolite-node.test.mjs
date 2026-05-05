import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { ShapeClient } from "../../clients/browser/electrolite.js";
import { createElectrolite, eq, inList, shape } from "./electrolite-node.js";

test("serves an authorized dynamic Shape through the native Rust binding", async () => {
  const { dir, electrolite } = setup();
  try {
    const request = new Request(
      "https://app.test/electrolite/v1/projectTodos/p1?offset=-1",
    );
    const response = await electrolite.handle(request, {
      user: { projects: new Set(["p1"]) },
    });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.match(body.shape_handle, /^[a-f0-9]{64}$/);
    delete body.shape_handle;
    assert.deepEqual(body, {
      type: "snapshot",
      key_columns: ["id"],
      rows: [{ id: 1, project_id: "p1", title: "ship electrolite", done: 0 }],
      offset: 2,
      up_to_date: true,
    });

    const denied = await electrolite.handle(request, {
      user: { projects: new Set(["p2"]) },
    });
    assert.equal(denied.status, 404);
    assert.deepEqual(await denied.json(), { error: "shape_not_found" });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("browser client uses the Node handler without keyColumns", async () => {
  const { dir, electrolite } = setup();
  try {
    const fetch = (url, init) => {
      return electrolite.handle(new Request(url, init), {
        user: { projects: new Set(["p1", "p2"]) },
      });
    };
    const client = new ShapeClient(
      "https://app.test/electrolite/v1/projectTodos/p1",
      {
        fetch,
        retry: { minDelayMs: 5, maxDelayMs: 20 },
      },
    );

    assert.equal(await client.request({ offset: -1 }), true);
    assert.deepEqual(client.currentRows(), [
      { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
    ]);

    const live = client.request({ offset: client.offset, live: true });
    await new Promise((resolve) => setTimeout(resolve, 25));
    electrolite.execute(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
      [3, "p1", "from node binding"],
    );
    assert.equal(await live, true);
    assert.deepEqual(client.currentRows(), [
      { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
      { id: 3, project_id: "p1", title: "from node binding", done: 0 },
    ]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("supports IN predicates and native write batches", async () => {
  const { dir, electrolite } = setup();
  try {
    electrolite.writeBatch([
      [
        "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
        [3, "p1", "batched p1"],
      ],
      [
        "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
        [4, "p2", "batched p2"],
      ],
    ]);

    const response = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/projectTodos/p1-p2?offset=-1"),
      { user: { projects: new Set(["p1", "p2"]) } },
    );
    assert.equal(response.status, 200);
    assert.deepEqual(
      (await response.json()).rows.map((row) => row.id),
      [1, 2, 3, 4],
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("returns resync_required after retained history is compacted", async () => {
  const { dir, electrolite } = setup();
  try {
    electrolite.compactLogToLastForTable("todos", 0);

    const response = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/projectTodos/p1?offset=0"),
      { user: { projects: new Set(["p1"]) } },
    );
    assert.equal(response.status, 409);
    assert.deepEqual(await response.json(), { error: "resync_required" });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

function setup() {
  const dir = mkdtempSync(join(tmpdir(), "electrolite-node-"));
  const dbPath = join(dir, "app.db");
  const electrolite = createElectrolite({
    dbPath,
    liveTimeoutMs: 500,
    pollIntervalMs: 10,
    shapes: {
      projectTodos: shape({
        table: "todos",
        columns: ["id", "project_id", "title", "done"],
        params: ["projectIds"],
        where: ({ params }) => {
          const projectIds = params.projectIds.split("-").filter(Boolean);
          return projectIds.length === 1
            ? eq("project_id", projectIds[0])
            : inList("project_id", projectIds);
        },
        scope: ({ params }) => `projects:${params.projectIds}`,
        authorize: ({ params, context }) => {
          const projectIds = params.projectIds.split("-").filter(Boolean);
          return projectIds.every((projectId) => context.user.projects.has(projectId));
        },
      }),
    },
  });
  electrolite.executeBatch(`
    CREATE TABLE todos (
      id INTEGER PRIMARY KEY,
      project_id TEXT NOT NULL,
      title TEXT NOT NULL,
      done BOOLEAN NOT NULL DEFAULT 0
    );
  `);
  electrolite.installTriggers("todos");
  electrolite.executeBatch(`
    INSERT INTO todos (id, project_id, title, done) VALUES
      (1, 'p1', 'ship electrolite', 0),
      (2, 'p2', 'other project', 0);
  `);
  return { dir, electrolite };
}
