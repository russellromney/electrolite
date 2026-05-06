// Cross-language client × engine matrix.
//
// Spawns each engine as a real HTTP server (or in-process for Node)
// and drives the same scenario against each from every client. Today
// the only client is the browser ShapeClient. New client languages are
// added by appending to CLIENTS.

import assert from "node:assert/strict";
import { ChildProcess, spawn, spawnSync } from "node:child_process";
import { createServer } from "node:http";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { ShapeClient } from "../clients/browser/electrolite.js";
import {
  createElectrolite,
  eq,
  gt,
  shape,
} from "../packages/electrolite-node/electrolite-node.ts";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const HOST = "127.0.0.1";

let nextPort = 5_300;
function pickPort(): number {
  return nextPort++;
}

interface Server {
  url: string;
  exec: (sql: string, args?: unknown[]) => Promise<void>;
  writeBatch: (statements: [string, unknown[]][]) => Promise<void>;
  stop: () => Promise<void>;
}

interface EngineDef {
  name: string;
  start: (port: number, dbPath: string) => Promise<Server>;
}

interface ClientDef {
  name: string;
  run: (server: Server) => Promise<void>;
}

// ---------- engines ----------

async function startNode(port: number, dbPath: string): Promise<Server> {
  const electrolite = createElectrolite({
    dbPath,
    liveTimeoutMs: 2_000,
    pollIntervalMs: 25,
    shapes: {
      projectTodos: shape({
        table: "todos",
        columns: ["id", "project_id", "title", "done"],
        params: ["projectId"],
        where: ({ params }) => eq("project_id", params.projectId),
        scope: ({ params }) => `project:${params.projectId}`,
        authorize: ({ params, context }) =>
          (context as any).user.projects.has(params.projectId),
      }),
      highIds: shape({
        table: "todos",
        columns: ["id", "project_id", "title", "done"],
        where: () => gt("id", 1),
      }),
    },
  });
  electrolite.executeBatch(`
    CREATE TABLE IF NOT EXISTS todos (
      id INTEGER PRIMARY KEY,
      project_id TEXT NOT NULL,
      title TEXT NOT NULL,
      done INTEGER NOT NULL DEFAULT 0
    );
  `);
  electrolite.installTriggers("todos");

  const httpServer = createServer(async (req, res) => {
    if (req.url?.startsWith("/electrolite/")) {
      const url = `http://${HOST}:${port}${req.url}`;
      const response = await electrolite.handle(new Request(url), {
        user: { projects: new Set(["p1", "p2"]) },
      });
      res.statusCode = response.status;
      res.setHeader("content-type", "application/json");
      res.end(await response.text());
      return;
    }
    if (req.method === "POST" && req.url?.startsWith("/_test/")) {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      const body = Buffer.concat(chunks).toString("utf-8");
      const payload = body ? JSON.parse(body) : {};
      try {
        if (req.url === "/_test/exec") {
          electrolite.execute(payload.sql, payload.args ?? []);
        } else if (req.url === "/_test/write_batch") {
          electrolite.writeBatch(payload.statements);
        }
        res.statusCode = 200;
        res.setHeader("content-type", "application/json");
        res.end(`{"ok":true}`);
      } catch (e: any) {
        res.statusCode = 500;
        res.end(JSON.stringify({ error: String(e?.message || e) }));
      }
      return;
    }
    res.statusCode = 404;
    res.end();
  });
  await new Promise<void>((resolve) => httpServer.listen(port, HOST, () => resolve()));

  return {
    url: `http://${HOST}:${port}`,
    exec: testExec(`http://${HOST}:${port}`),
    writeBatch: testWriteBatch(`http://${HOST}:${port}`),
    stop: () =>
      new Promise<void>((resolve) => {
        httpServer.close(() => resolve());
      }),
  };
}

async function startSubprocess(
  command: string,
  args: string[],
  port: number,
  options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
): Promise<{ proc: ChildProcess; ready: Promise<void> }> {
  const proc = spawn(command, args, {
    cwd: options.cwd,
    env: { ...process.env, ...(options.env ?? {}) },
    stdio: ["ignore", "pipe", "pipe"],
  });

  const ready = new Promise<void>((resolve, reject) => {
    let buffer = "";
    let done = false;
    const onData = (chunk: Buffer) => {
      buffer += chunk.toString("utf-8");
      if (!done && buffer.includes(`listening on ${port}`)) {
        done = true;
        resolve();
      }
    };
    proc.stdout?.on("data", onData);
    proc.stderr?.on("data", onData);
    proc.on("error", reject);
    proc.on("exit", (code) => {
      if (!done) reject(new Error(`server exited early (code ${code}): ${buffer}`));
    });
    setTimeout(() => {
      if (!done) reject(new Error(`server did not become ready in time: ${buffer}`));
    }, 30_000);
  });

  return { proc, ready };
}

function subprocessServer(
  command: string,
  args: string[],
  port: number,
  options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
): () => Promise<Server> {
  return async () => {
    const { proc, ready } = await startSubprocess(command, args, port, options);
    await ready;
    const url = `http://${HOST}:${port}`;
    // Sanity ping before handing off.
    await waitForHealth(url);
    return {
      url,
      exec: testExec(url),
      writeBatch: testWriteBatch(url),
      stop: () =>
        new Promise<void>((resolve) => {
          proc.once("exit", () => resolve());
          proc.kill("SIGTERM");
          setTimeout(() => {
            proc.kill("SIGKILL");
            resolve();
          }, 1_500);
        }),
    };
  };
}

async function waitForHealth(url: string) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${url}/electrolite/v1/projectTodos/p1?offset=-1`);
      if (r.status === 200) return;
    } catch {}
    await sleep(25);
  }
  throw new Error(`server at ${url} never responded`);
}

function testExec(url: string) {
  return async (sql: string, args: unknown[] = []) => {
    const r = await fetch(`${url}/_test/exec`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sql, args }),
    });
    if (!r.ok) throw new Error(`exec failed: ${r.status} ${await r.text()}`);
  };
}

function testWriteBatch(url: string) {
  return async (statements: [string, unknown[]][]) => {
    const r = await fetch(`${url}/_test/write_batch`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ statements }),
    });
    if (!r.ok) throw new Error(`write_batch failed: ${r.status} ${await r.text()}`);
  };
}

function startPython(port: number, dbPath: string): Promise<Server> {
  return subprocessServer(
    "python3",
    [join(ROOT, "engines/python/server/server.py"), "--port", String(port), "--db", dbPath],
    port,
  )();
}

function startGo(port: number, dbPath: string): Promise<Server> {
  const binary = join(ROOT, "engines/go/server/server-bin");
  return subprocessServer(
    binary,
    ["--port", String(port), "--db", dbPath],
    port,
    { cwd: join(ROOT, "engines/go") },
  )();
}

function startRust(port: number, dbPath: string): Promise<Server> {
  const binary = join(ROOT, "engines/rust/target/debug/electrolite-server");
  return subprocessServer(
    binary,
    ["--port", String(port), "--db", dbPath],
    port,
  )();
}

function startElixir(port: number, dbPath: string): Promise<Server> {
  return subprocessServer(
    "mix",
    ["run", "--no-halt", "server/run.exs", "--port", String(port), "--db", dbPath],
    port,
    { cwd: join(ROOT, "engines/elixir"), env: { MIX_ENV: "dev" } },
  )();
}

const ENGINES: EngineDef[] = [
  { name: "node", start: startNode },
  { name: "python", start: startPython },
  { name: "rust", start: startRust },
  { name: "go", start: startGo },
  { name: "elixir", start: startElixir },
];

// ---------- clients ----------

async function runBrowserClient(server: Server) {
  // Empty-snapshot baseline.
  const client = new ShapeClient(`${server.url}/electrolite/v1/projectTodos/p1`, {
    retry: { minDelayMs: 5, maxDelayMs: 20 },
  });

  assert.equal(await client.request({ offset: -1 }), true);
  assert.deepEqual(client.currentRows(), []);

  // Live insert: write a row, see it materialize.
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

  // Live update.
  live = client.request({ offset: client.offset, live: true });
  await sleep(25);
  await server.exec("UPDATE todos SET title = ? WHERE id = ?", ["renamed", 1]);
  assert.equal(await live, true);
  assert.equal(client.currentRows()[0].title, "renamed");

  // Live delete.
  live = client.request({ offset: client.offset, live: true });
  await sleep(25);
  await server.exec("DELETE FROM todos WHERE id = ?", [1]);
  assert.equal(await live, true);
  assert.deepEqual(client.currentRows(), []);

  // Write batch — two rows arrive together.
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

  // Cross-shape: a row that doesn't match the predicate must NOT show up.
  live = client.request({ offset: client.offset, live: true });
  await sleep(25);
  await server.exec(
    "INSERT INTO todos (id, project_id, title, done) VALUES (?, ?, ?, 0)",
    [99, "p2", "ignore me"],
  );
  // Either the live request times out and returns false, or it returns
  // true but the materialized rows must not include id 99. Both are
  // acceptable per protocol.
  await live;
  assert.equal(
    client.currentRows().some((r: any) => r.id === 99),
    false,
  );
}

const CLIENTS: ClientDef[] = [
  { name: "browser", run: runBrowserClient },
];

// ---------- driver ----------

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// Pre-build the Go and Rust binaries once before tests run, so each
// test only pays the spawn cost.
function ensureBuilt(): void {
  if (!process.env.ELECTROLITE_MATRIX_SKIP_BUILD) {
    const goBin = join(ROOT, "engines/go/server/server-bin");
    if (!existsSync(goBin)) {
      const r = spawnSync(
        "go",
        ["build", "-o", goBin, "./server"],
        { cwd: join(ROOT, "engines/go"), stdio: "inherit" },
      );
      if (r.status !== 0) throw new Error("go build failed");
    }
    const rustBin = join(ROOT, "engines/rust/target/debug/electrolite-server");
    if (!existsSync(rustBin)) {
      const r = spawnSync(
        "cargo",
        [
          "build",
          "--manifest-path",
          join(ROOT, "engines/rust/Cargo.toml"),
          "--bin",
          "electrolite-server",
        ],
        { stdio: "inherit" },
      );
      if (r.status !== 0) throw new Error("cargo build failed");
    }
  }
}

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
