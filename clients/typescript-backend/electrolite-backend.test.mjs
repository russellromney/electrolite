import assert from "node:assert/strict";
import test from "node:test";
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
