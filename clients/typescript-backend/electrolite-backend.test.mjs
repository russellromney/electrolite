import assert from "node:assert/strict";
import { once } from "node:events";
import { spawn } from "node:child_process";
import test from "node:test";
import { ShapeClient } from "../browser/electrolite.js";
import {
  createElectroliteProxy,
  parseElectroliteRequest,
  trustedShapeHeaders,
} from "./electrolite-backend.js";

test("parses static and factory Electrolite routes", () => {
  assert.deepEqual(
    routeSummary(
      parseElectroliteRequest(
        "https://app.test/electrolite/v1/shape/activeUsers?offset=7&live=true",
      ),
    ),
    {
      kind: "shape",
      name: "activeUsers",
      path: "",
      offset: 7,
      live: true,
      forwardPath: "/electrolite/v1/shape/activeUsers",
    },
  );

  assert.deepEqual(
    routeSummary(
      parseElectroliteRequest(
        "https://app.test/electrolite/v1/factory/projectTodos/p1?offset=-1",
      ),
    ),
    {
      kind: "factory",
      name: "projectTodos",
      path: "p1",
      offset: -1,
      live: false,
      forwardPath: "/electrolite/v1/factory/projectTodos/p1",
    },
  );
});

test("authorizes and forwards requests with scoped headers", async () => {
  const forwarded = [];
  const proxy = createElectroliteProxy({
    origin: "http://127.0.0.1:5137",
    authorize: async (context) => {
      assert.equal(context.kind, "factory");
      assert.equal(context.name, "projectTodos");
      assert.equal(context.path, "p1");
      assert.equal(context.offset, 42);
      assert.equal(context.live, true);
      return {
        allow: true,
        headers: {
          "x-electrolite-project": context.path,
          "x-electrolite-scope": `project:${context.path}`,
        },
      };
    },
    fetch: async (url, init) => {
      forwarded.push({ url: String(url), headers: init.headers });
      return Response.json({ type: "replay", messages: [], offset: 42 });
    },
  });

  const response = await proxy(
    new Request(
      "https://app.test/electrolite/v1/factory/projectTodos/p1?offset=42&live=true",
      {
        headers: {
          accept: "application/json",
          cookie: "session=private",
          authorization: "Bearer private",
        },
      },
    ),
  );

  assert.equal(response.status, 200);
  assert.equal(
    forwarded[0].url,
    "http://127.0.0.1:5137/electrolite/v1/factory/projectTodos/p1?offset=42&live=true",
  );
  assert.equal(forwarded[0].headers.get("accept"), "application/json");
  assert.equal(forwarded[0].headers.get("cookie"), null);
  assert.equal(forwarded[0].headers.get("authorization"), null);
  assert.equal(forwarded[0].headers.get("x-electrolite-project"), "p1");
  assert.equal(forwarded[0].headers.get("x-electrolite-scope"), "project:p1");
});

test("builds trusted shape headers for a TypeScript-defined Shape", () => {
  assert.deepEqual(
    trustedShapeHeaders({
      name: "projectTodos/p1",
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      predicate: { type: "eq", column: "project_id", value: "p1" },
      auth_scope: "project:p1",
      schema_version: 1,
    }),
    {
      "x-electrolite-shape-name": "projectTodos/p1",
      "x-electrolite-table": "todos",
      "x-electrolite-columns": '["id","project_id","title","done"]',
      "x-electrolite-predicate": '{"type":"eq","column":"project_id","value":"p1"}',
      "x-electrolite-auth-scope": "project:p1",
      "x-electrolite-schema-version": "1",
    },
  );
});

test("builds trusted shape headers for IN predicates", () => {
  assert.equal(
    trustedShapeHeaders({
      name: "projectTodos/p1-p2",
      table: "todos",
      columns: ["id", "project_id", "title", "done"],
      predicate: { type: "in", column: "project_id", values: ["p1", "p2"] },
      auth_scope: "projects:p1,p2",
      schema_version: 1,
    })["x-electrolite-predicate"],
    '{"type":"in","column":"project_id","values":["p1","p2"]}',
  );
});

test("supports the trusted factory route used by TypeScript backends", async () => {
  const forwarded = [];
  const proxy = createElectroliteProxy({
    origin: "http://127.0.0.1:5137",
    authorize: ({ kind, name, path }) => {
      assert.equal(kind, "factory");
      assert.equal(name, "trusted");
      assert.equal(path, "projectTodos/p1");

      const [shapeName, projectId] = path.split("/");
      return {
        allow: true,
        headers: {
          ...trustedShapeHeaders({
            name: `${shapeName}/${projectId}`,
            table: "todos",
            columns: ["id", "project_id", "title", "done"],
            predicate: { type: "eq", column: "project_id", value: projectId },
            auth_scope: `project:${projectId}`,
            schema_version: 1,
          }),
          "x-electrolite-scope": `project:${projectId}`,
        },
      };
    },
    fetch: async (url, init) => {
      forwarded.push({ url: String(url), headers: init.headers });
      return Response.json({ type: "snapshot", rows: [], offset: 0 });
    },
  });

  const response = await proxy(
    new Request(
      "https://app.test/electrolite/v1/factory/trusted/projectTodos/p1?offset=-1",
    ),
  );

  assert.equal(response.status, 200);
  assert.equal(
    forwarded[0].url,
    "http://127.0.0.1:5137/electrolite/v1/factory/trusted/projectTodos/p1?offset=-1",
  );
  assert.equal(forwarded[0].headers.get("x-electrolite-shape-name"), "projectTodos/p1");
  assert.equal(forwarded[0].headers.get("x-electrolite-scope"), "project:p1");
});

test("denies without forwarding to the Electrolite origin", async () => {
  let forwarded = false;
  const proxy = createElectroliteProxy({
    origin: "http://127.0.0.1:5137",
    authorize: () => false,
    fetch: async () => {
      forwarded = true;
      return Response.json({});
    },
  });

  const response = await proxy(
    new Request("https://app.test/electrolite/v1/shape/private?offset=-1"),
  );

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), { error: "shape_not_found" });
  assert.equal(forwarded, false);
});

test("returns 404 for non Electrolite paths", async () => {
  const proxy = createElectroliteProxy({
    origin: "http://127.0.0.1:5137",
    authorize: () => true,
    fetch: async () => Response.json({}),
  });

  const response = await proxy(new Request("https://app.test/not-electrolite"));

  assert.equal(response.status, 404);
});

test("rejects non GET requests before forwarding", async () => {
  let forwarded = false;
  const proxy = createElectroliteProxy({
    origin: "http://127.0.0.1:5137",
    authorize: () => true,
    fetch: async () => {
      forwarded = true;
      return Response.json({});
    },
  });

  const response = await proxy(
    new Request("https://app.test/electrolite/v1/shape/activeUsers?offset=-1", {
      method: "POST",
    }),
  );

  assert.equal(response.status, 405);
  assert.deepEqual(await response.json(), { error: "method_not_allowed" });
  assert.equal(forwarded, false);
});

test("treats malformed encoded paths as not found", () => {
  assert.equal(
    parseElectroliteRequest(
      "https://app.test/electrolite/v1/factory/projectTodos/%E0%A4%A?offset=-1",
    ),
    null,
  );
});

test("e2e TypeScript backend proxy materializes a trusted Shape", async (t) => {
  const origin = await startRustOrigin(t);
  const session = { userId: "u1", projects: new Set(["p1"]) };
  const proxy = createElectroliteProxy({
    origin,
    authorize: async ({ kind, name, path }) => {
      if (kind !== "factory" || name !== "trusted") {
        return false;
      }
      const [shapeName, projectId] = path.split("/");
      const projectIds = projectId.split("-").filter(Boolean);
      if (
        shapeName !== "projectTodos" ||
        projectIds.length === 0 ||
        !projectIds.every((id) => session.projects.has(id))
      ) {
        return false;
      }
      const predicate =
        projectIds.length === 1
          ? { type: "eq", column: "project_id", value: projectIds[0] }
          : { type: "in", column: "project_id", values: projectIds };
      const authScope =
        projectIds.length === 1
          ? `project:${projectIds[0]}`
          : `projects:${projectIds.join(",")}`;
      return {
        allow: true,
        headers: {
          ...trustedShapeHeaders({
            name: `${shapeName}/${projectId}`,
            table: "todos",
            columns: ["id", "project_id", "title", "done"],
            predicate,
            auth_scope: authScope,
            schema_version: 1,
          }),
          "x-electrolite-scope": authScope,
        },
      };
    },
  });
  const fetchThroughTypeScriptBackend = (url, init) => {
    const request = new Request(url, init);
    return proxy(request);
  };
  const client = new ShapeClient(
    "https://app.test/electrolite/v1/factory/trusted/projectTodos/p1",
    {
      fetch: fetchThroughTypeScriptBackend,
      retry: { minDelayMs: 5, maxDelayMs: 20 },
    },
  );

  assert.equal(await client.request({ offset: -1 }), true);
  assert.deepEqual(client.currentRows(), [
    { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
  ]);

  const denied = await proxy(
    new Request("https://app.test/electrolite/v1/factory/trusted/projectTodos/p2?offset=-1"),
  );
  assert.equal(denied.status, 404);

  const ignoredLive = client.request({ offset: client.offset, live: true });
  await fetch(`${origin}/test/update-p2`, { method: "POST" });
  assert.equal(await ignoredLive, false);
  assert.deepEqual(client.currentRows(), [
    { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
  ]);

  const visibleLive = client.request({ offset: client.offset, live: true });
  await fetch(`${origin}/test/insert-p1`, { method: "POST" });
  assert.equal(await visibleLive, true);
  assert.deepEqual(client.currentRows(), [
    { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
    { id: 3, project_id: "p1", title: "from ts backend", done: 0 },
  ]);

  session.projects.add("p2");
  const multiProjectClient = new ShapeClient(
    "https://app.test/electrolite/v1/factory/trusted/projectTodos/p1-p2",
    {
      fetch: fetchThroughTypeScriptBackend,
      retry: { minDelayMs: 5, maxDelayMs: 20 },
    },
  );
  assert.equal(await multiProjectClient.request({ offset: -1 }), true);
  assert.deepEqual(
    multiProjectClient.currentRows().sort((a, b) => a.id - b.id),
    [
      { id: 1, project_id: "p1", title: "ship electrolite", done: 0 },
      { id: 2, project_id: "p2", title: "not visible", done: 0 },
      { id: 3, project_id: "p1", title: "from ts backend", done: 0 },
    ],
  );
});

function routeSummary(route) {
  return {
    kind: route.kind,
    name: route.name,
    path: route.path,
    offset: route.offset,
    live: route.live,
    forwardPath: route.forwardPath,
  };
}

async function startRustOrigin(t) {
  const child = spawn(
    "cargo",
    ["run", "-q", "-p", "electrolite-server", "--example", "typescript_backend_origin"],
    {
      cwd: new URL("../..", import.meta.url),
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  t.after(() => child.kill());

  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  child.stdout.setEncoding("utf8");

  const timeout = setTimeout(() => child.kill(), 30_000);
  try {
    let stdout = "";
    while (true) {
      const [chunk] = await Promise.race([
        once(child.stdout, "data"),
        once(child, "exit").then(([code]) => {
          throw new Error(`Rust origin exited with ${code}: ${stderr}`);
        }),
      ]);
      stdout += chunk;
      const match = stdout.match(/ELECTROLITE_ORIGIN=(http:\/\/[^\s]+)/);
      if (match) {
        return match[1];
      }
    }
  } finally {
    clearTimeout(timeout);
  }
}
