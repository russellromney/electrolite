import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { ShapeClient } from "../../clients/browser/electrolite.js";
import { createElectrolite, all, eq, inList, shape } from "./electrolite-node.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("serves an authorized dynamic Shape", async () => {
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
    assert.match(body.log_id, /^[a-f0-9]{32}$/);
    delete body.shape_handle;
    delete body.log_id;
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
    await sleep(25);
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

test("live requests only wake for writes visible to that Shape", async () => {
  const { dir, electrolite } = setup({ liveTimeoutMs: 80, pollIntervalMs: 1_000 });
  try {
    const fetch = (url, init) => {
      return electrolite.handle(new Request(url, init), {
        user: { projects: new Set(["p1", "p2"]) },
      });
    };
    const p1Client = new ShapeClient(
      "https://app.test/electrolite/v1/projectTodos/p1",
      {
        fetch,
        retry: { minDelayMs: 5, maxDelayMs: 20 },
      },
    );
    const p2Client = new ShapeClient(
      "https://app.test/electrolite/v1/projectTodos/p2",
      {
        fetch,
        retry: { minDelayMs: 5, maxDelayMs: 20 },
      },
    );

    assert.equal(await p1Client.request({ offset: -1 }), true);
    assert.equal(await p2Client.request({ offset: -1 }), true);

    const p1Live = p1Client.request({ offset: p1Client.offset, live: true });
    const p2Live = p2Client.request({ offset: p2Client.offset, live: true });
    await sleep(25);
    electrolite.execute(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
      [3, "p1", "targeted p1"],
    );

    assert.equal(await p1Live, true);
    assert.equal(await p2Live, false);
    assert.deepEqual(p1Client.currentRows(), [
      { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
      { id: 3, project_id: "p1", title: "targeted p1", done: 0 },
    ]);
    assert.deepEqual(p2Client.currentRows(), [
      { id: 2, project_id: "p2", title: "other project", done: 0 },
    ]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("moving a row between Shapes wakes both affected browser clients", async () => {
  const { dir, electrolite } = setup({ liveTimeoutMs: 120, pollIntervalMs: 1_000 });
  try {
    const fetch = (url, init) => {
      return electrolite.handle(new Request(url, init), {
        user: { projects: new Set(["p1", "p2"]) },
      });
    };
    const p1Client = new ShapeClient(
      "https://app.test/electrolite/v1/projectTodos/p1",
      {
        fetch,
        retry: { minDelayMs: 5, maxDelayMs: 20 },
      },
    );
    const p2Client = new ShapeClient(
      "https://app.test/electrolite/v1/projectTodos/p2",
      {
        fetch,
        retry: { minDelayMs: 5, maxDelayMs: 20 },
      },
    );

    assert.equal(await p1Client.request({ offset: -1 }), true);
    assert.equal(await p2Client.request({ offset: -1 }), true);

    const p1Live = p1Client.request({ offset: p1Client.offset, live: true });
    const p2Live = p2Client.request({ offset: p2Client.offset, live: true });
    await sleep(25);
    electrolite.execute(
      "UPDATE todos SET project_id = ?1, title = ?2 WHERE id = ?3",
      ["p2", "moved to p2", 1],
    );

    assert.equal(await p1Live, true);
    assert.equal(await p2Live, true);
    assert.deepEqual(p1Client.currentRows(), []);
    assert.deepEqual(p2Client.currentRows(), [
      { id: 2, project_id: "p2", title: "other project", done: 0 },
      { id: 1, project_id: "p2", title: "moved to p2", done: 0 },
    ]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("browser client stages bounded replay pages from the Node backend", async () => {
  const { dir, electrolite } = setup({ replayLimit: 1 });
  try {
    const fetch = (url, init) => {
      return electrolite.handle(new Request(url, init), {
        user: { projects: new Set(["p1"]) },
      });
    };
    const client = new ShapeClient(
      "https://app.test/electrolite/v1/projectTodos/p1",
      {
        fetch,
        retry: { minDelayMs: 5, maxDelayMs: 20 },
      },
    );
    const seen = [];
    client.subscribe((rows) => seen.push(rows));

    assert.equal(await client.request({ offset: -1 }), true);
    const snapshotOffset = client.offset;
    electrolite.execute(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
      [3, "p1", "bounded one"],
    );
    electrolite.execute(
      "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
      [4, "p1", "bounded two"],
    );

    assert.equal(await client.request({ offset: snapshotOffset }), false);
    assert.equal(client.offset, snapshotOffset + 1);
    assert.deepEqual(client.currentRows(), [
      { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
    ]);

    assert.equal(await client.request({ offset: client.offset }), true);
    assert.deepEqual(client.currentRows(), [
      { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
      { id: 3, project_id: "p1", title: "bounded one", done: 0 },
      { id: 4, project_id: "p1", title: "bounded two", done: 0 },
    ]);
    assert.equal(seen.length, 3);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("supports IN predicates and Electrolite write batches", async () => {
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

test("returns resync_required when a replay presents the wrong log id", async () => {
  const { dir, electrolite } = setup();
  try {
    const response = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/projectTodos/p1?offset=2&log_id=wrong"),
      { user: { projects: new Set(["p1"]) } },
    );
    assert.equal(response.status, 409);
    assert.deepEqual(await response.json(), { error: "resync_required" });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("accepts replay when the client presents the current log id", async () => {
  const { dir, electrolite } = setup();
  try {
    const snapshot = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/projectTodos/p1?offset=-1"),
      { user: { projects: new Set(["p1"]) } },
    );
    const { log_id: logId, offset } = await snapshot.json();
    const response = await electrolite.handle(
      new Request(
        `https://app.test/electrolite/v1/projectTodos/p1?offset=${offset}&log_id=${logId}`,
      ),
      { user: { projects: new Set(["p1"]) } },
    );
    assert.equal(response.status, 200);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("shape handles are normalized against SQLite schema", async () => {
  const dir = mkdtempSync(join(tmpdir(), "electrolite-node-"));
  const dbPath = join(dir, "app.db");
  const electrolite = createElectrolite({
    dbPath,
    shapes: {
      enabledFlags: shape({
        table: "flags",
        columns: ["id", "enabled"],
        params: ["value"],
        where: ({ params }) => eq("enabled", params.value === "true" ? true : 1),
        authorize: () => true,
      }),
      allFlags: shape({
        table: "flags",
        columns: ["id", "enabled"],
        where: () => all(),
      }),
    },
  });
  try {
    electrolite.executeBatch(`
      CREATE TABLE flags (
        id INTEGER PRIMARY KEY,
        enabled BOOLEAN NOT NULL
      );
    `);
    electrolite.installTriggers("flags");

    const boolResponse = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/enabledFlags/true?offset=-1"),
      {},
    );
    const intResponse = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/enabledFlags/one?offset=-1"),
      {},
    );
    assert.equal(boolResponse.status, 200);
    assert.equal(intResponse.status, 200);
    assert.equal(
      (await boolResponse.json()).shape_handle,
      (await intResponse.json()).shape_handle,
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("primary-key updates replay as delete then insert", async () => {
  const { dir, electrolite } = setup();
  try {
    const snapshot = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/projectTodos/p1?offset=-1"),
      { user: { projects: new Set(["p1"]) } },
    );
    const { offset, log_id: logId } = await snapshot.json();

    electrolite.execute("UPDATE todos SET id = ?1 WHERE id = ?2", [10, 1]);
    const response = await electrolite.handle(
      new Request(
        `https://app.test/electrolite/v1/projectTodos/p1?offset=${offset}&log_id=${logId}`,
      ),
      { user: { projects: new Set(["p1"]) } },
    );
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.deepEqual(body.messages.map(({ type, key }) => ({ type, key })), [
      { type: "delete", key: { id: 1 } },
      { type: "insert", key: { id: 10 } },
    ]);
    assert.equal(body.messages[0].batch_id, body.messages[1].batch_id);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("null equality and IN null match in snapshots and replay", async () => {
  const dir = mkdtempSync(join(tmpdir(), "electrolite-node-"));
  const dbPath = join(dir, "app.db");
  const electrolite = createElectrolite({
    dbPath,
    shapes: {
      unnicknamedPeople: shape({
        table: "people",
        columns: ["id", "name", "nickname"],
        where: () => eq("nickname", null),
      }),
      selectedTodos: shape({
        table: "nullable_todos",
        columns: ["id", "project_id", "title"],
        where: () => inList("project_id", ["p1", null]),
      }),
    },
  });
  try {
    electrolite.executeBatch(`
      CREATE TABLE people (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        nickname TEXT
      );
      CREATE TABLE nullable_todos (
        id INTEGER PRIMARY KEY,
        project_id TEXT,
        title TEXT NOT NULL
      );
    `);
    electrolite.installTriggers("people");
    electrolite.installTriggers("nullable_todos");
    electrolite.executeBatch(`
      INSERT INTO people (id, name, nickname) VALUES
        (1, 'Ada', NULL),
        (2, 'Grace', 'Amazing Grace');
      INSERT INTO nullable_todos (id, project_id, title) VALUES
        (1, 'p1', 'one'),
        (2, 'p2', 'two'),
        (3, NULL, 'null project');
    `);

    const peopleSnapshot = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/unnicknamedPeople?offset=-1"),
      {},
    );
    assert.equal(peopleSnapshot.status, 200);
    const peopleBody = await peopleSnapshot.json();
    assert.deepEqual(peopleBody.rows, [
      { id: 1, name: "Ada", nickname: null },
    ]);

    electrolite.execute("UPDATE people SET nickname = NULL WHERE id = ?1", [2]);
    const peopleReplay = await electrolite.handle(
      new Request(
        `https://app.test/electrolite/v1/unnicknamedPeople?offset=${peopleBody.offset}&log_id=${peopleBody.log_id}`,
      ),
      {},
    );
    assert.equal(peopleReplay.status, 200);
    assert.deepEqual((await peopleReplay.json()).messages.map(({ type, key }) => ({ type, key })), [
      { type: "insert", key: { id: 2 } },
    ]);

    const todoSnapshot = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/selectedTodos?offset=-1"),
      {},
    );
    assert.equal(todoSnapshot.status, 200);
    assert.deepEqual((await todoSnapshot.json()).rows.map((row) => row.id), [1, 3]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("predicate type policy rejects ambiguous values", async () => {
  const dir = mkdtempSync(join(tmpdir(), "electrolite-node-"));
  const dbPath = join(dir, "app.db");
  const electrolite = createElectrolite({
    dbPath,
    shapes: {
      boolOnInteger: shape({
        table: "typed_values",
        columns: ["id", "count", "label"],
        where: () => eq("count", true),
      }),
      numericString: shape({
        table: "typed_values",
        columns: ["id", "count", "label"],
        where: () => eq("count", "1"),
      }),
      textNumber: shape({
        table: "typed_values",
        columns: ["id", "count", "label"],
        where: () => eq("label", 1),
      }),
    },
  });
  try {
    electrolite.executeBatch(`
      CREATE TABLE typed_values (
        id INTEGER PRIMARY KEY,
        count INTEGER NOT NULL,
        label TEXT NOT NULL
      );
    `);
    electrolite.installTriggers("typed_values");

    for (const shapeName of ["boolOnInteger", "numericString", "textNumber"]) {
      const response = await electrolite.handle(
        new Request(`https://app.test/electrolite/v1/${shapeName}?offset=-1`),
        {},
      );
      assert.equal(response.status, 500);
      assert.deepEqual(await response.json(), { error: "internal_server_error" });
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("non-id and composite primary keys are exposed as key metadata and replay keys", async () => {
  const dir = mkdtempSync(join(tmpdir(), "electrolite-node-"));
  const dbPath = join(dir, "app.db");
  const electrolite = createElectrolite({
    dbPath,
    shapes: {
      publicProjects: shape({
        table: "projects",
        columns: ["slug", "title", "public"],
        where: () => eq("public", 1),
      }),
      memberships: shape({
        table: "memberships",
        columns: ["account_id", "user_id", "role"],
        where: () => all(),
      }),
    },
  });
  try {
    electrolite.executeBatch(`
      CREATE TABLE projects (
        slug TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        public INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE memberships (
        account_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        role TEXT NOT NULL,
        PRIMARY KEY (account_id, user_id)
      );
    `);
    electrolite.installTriggers("projects");
    electrolite.installTriggers("memberships");
    electrolite.execute("INSERT INTO projects (slug, title, public) VALUES (?1, ?2, 1)", [
      "electrolite",
      "Electrolite",
    ]);
    electrolite.execute(
      "INSERT INTO memberships (account_id, user_id, role) VALUES (?1, ?2, ?3)",
      [7, 11, "admin"],
    );

    const projectsSnapshot = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/publicProjects?offset=-1"),
      {},
    );
    assert.equal(projectsSnapshot.status, 200);
    const projectsBody = await projectsSnapshot.json();
    assert.deepEqual(projectsBody.key_columns, ["slug"]);
    assert.deepEqual(projectsBody.rows, [
      { slug: "electrolite", title: "Electrolite", public: 1 },
    ]);

    const membershipsSnapshot = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/memberships?offset=-1"),
      {},
    );
    assert.equal(membershipsSnapshot.status, 200);
    const membershipsBody = await membershipsSnapshot.json();
    assert.deepEqual(membershipsBody.key_columns, ["account_id", "user_id"]);

    electrolite.execute(
      "UPDATE memberships SET role = ?1 WHERE account_id = ?2 AND user_id = ?3",
      ["member", 7, 11],
    );
    const membershipsReplay = await electrolite.handle(
      new Request(
        `https://app.test/electrolite/v1/memberships?offset=${membershipsBody.offset}&log_id=${membershipsBody.log_id}`,
      ),
      {},
    );
    assert.equal(membershipsReplay.status, 200);
    assert.deepEqual((await membershipsReplay.json()).messages.map(({ type, key }) => ({ type, key })), [
      { type: "update", key: { account_id: 7, user_id: 11 } },
    ]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("missing triggers and missing key columns fail instead of silently serving broken Shapes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "electrolite-node-"));
  const dbPath = join(dir, "app.db");
  const electrolite = createElectrolite({
    dbPath,
    shapes: {
      unwatchedUsers: shape({
        table: "users",
        columns: ["id", "name"],
        where: () => all(),
      }),
      missingKeyUsers: shape({
        table: "users",
        columns: ["name"],
        where: () => all(),
      }),
    },
  });
  try {
    electrolite.executeBatch(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      );
      INSERT INTO users (id, name) VALUES (1, 'Ada');
    `);
    let response = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/unwatchedUsers?offset=-1"),
      {},
    );
    assert.equal(response.status, 500);

    electrolite.installTriggers("users");
    response = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/missingKeyUsers?offset=-1"),
      {},
    );
    assert.equal(response.status, 500);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("limit zero, table retention, and rollback semantics match the public protocol", async () => {
  const { dir, electrolite } = setup({ replayLimit: 1 });
  try {
    const snapshot = await electrolite.handle(
      new Request("https://app.test/electrolite/v1/projectTodos/p1?offset=-1"),
      { user: { projects: new Set(["p1"]) } },
    );
    const { offset, log_id: logId } = await snapshot.json();

    electrolite.execute("INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)", [
      3,
      "p1",
      "limit one",
    ]);
    const replay = JSON.parse(
      electrolite.engine.replay(
        JSON.stringify({
          name: "projectTodos/p1",
          table: "todos",
          columns: ["id", "project_id", "title", "done"],
          predicate: eq("project_id", "p1"),
          auth_scope: "projects:p1",
          schema_version: 1,
        }),
        offset,
        0,
      ),
    );
    assert.equal(replay.offset, offset + 1);
    assert.equal(replay.up_to_date, true);

    electrolite.executeBatch(`
      CREATE TABLE projects (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      );
    `);
    electrolite.installTriggers("projects");
    for (let id = 1; id <= 5; id += 1) {
      electrolite.execute("INSERT INTO projects (id, name) VALUES (?1, ?2)", [
        id,
        `project ${id}`,
      ]);
    }
    electrolite.compactLogToLastForTable("projects", 0);
    const quietReplay = await electrolite.handle(
      new Request(
        `https://app.test/electrolite/v1/projectTodos/p1?offset=${replay.offset}&log_id=${logId}`,
      ),
      { user: { projects: new Set(["p1"]) } },
    );
    assert.equal(quietReplay.status, 200);

    const beforeFailedBatch = electrolite.highWaterMark();
    assert.throws(() => {
      electrolite.writeBatch([
        [
          "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
          [50, "p1", "rolled back"],
        ],
        [
          "INSERT INTO todos (id, project_id, title, done) VALUES (?1, ?2, ?3, 0)",
          [1, "p1", "duplicate"],
        ],
      ]);
    });
    assert.equal(electrolite.highWaterMark(), beforeFailedBatch);

    electrolite.executeBatch(`
      BEGIN;
      INSERT INTO todos (id, project_id, title, done) VALUES (51, 'p1', 'raw rollback', 0);
      ROLLBACK;
    `);
    assert.equal(electrolite.highWaterMark(), beforeFailedBatch);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("installs replay indexes for table-filtered log reads", async () => {
  const { dir, electrolite } = setup();
  try {
    const indexes = electrolite.engine.db.prepare(`
      SELECT name FROM sqlite_master
      WHERE type = 'index' AND tbl_name = '_electrolite_log'
      ORDER BY name
    `).all().map((row) => row.name);

    assert.ok(indexes.includes("_electrolite_log_table_seq_idx"));
    assert.ok(indexes.includes("_electrolite_log_table_batch_seq_idx"));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

function setup(options = {}) {
  const dir = mkdtempSync(join(tmpdir(), "electrolite-node-"));
  const dbPath = join(dir, "app.db");
  const electrolite = createElectrolite({
    dbPath,
    liveTimeoutMs: options.liveTimeoutMs ?? 500,
    pollIntervalMs: options.pollIntervalMs ?? 10,
    replayLimit: options.replayLimit,
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
