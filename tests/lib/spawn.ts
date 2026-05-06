// Engine spawn helpers shared between the matrix test and the
// conformance harness. Each engine starts a real HTTP server bound
// to 127.0.0.1:<port> with the canonical projectTodos / highIds
// shapes registered.

import { ChildProcess, spawn, spawnSync } from "node:child_process";
import { createServer } from "node:http";
import { existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  createElectrolite,
  eq,
  gt,
  shape,
} from "../../packages/electrolite-node/electrolite-node.ts";

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
export const HOST = "127.0.0.1";

export interface Server {
  url: string;
  exec: (sql: string, args?: unknown[]) => Promise<void>;
  writeBatch: (statements: [string, unknown[]][]) => Promise<void>;
  stop: () => Promise<void>;
}

export interface EngineDef {
  name: string;
  start: (port: number, dbPath: string) => Promise<Server>;
}

let nextPort = 5_400;
export function pickPort(): number {
  return nextPort++;
}

export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------- node (in-process http wrapper) ----------

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
        } else if (req.url === "/_test/seed") {
          electrolite.executeBatch(payload.sql);
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

  const url = `http://${HOST}:${port}`;
  return {
    url,
    exec: testExec(url),
    writeBatch: testWriteBatch(url),
    stop: () =>
      new Promise<void>((resolve) => {
        httpServer.close(() => resolve());
      }),
  };
}

// ---------- subprocess servers ----------

async function startSubprocessRaw(
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
    const { proc, ready } = await startSubprocessRaw(command, args, port, options);
    await ready;
    const url = `http://${HOST}:${port}`;
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
  // Built binary may live under engines/rust/target or under the
  // workspace root target/, depending on how cargo was invoked.
  const candidates = [
    join(ROOT, "engines/rust/target/debug/electrolite-server"),
    join(ROOT, "target/debug/electrolite-server"),
  ];
  const binary = candidates.find(existsSync) ?? candidates[0];
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

export const ENGINES: EngineDef[] = [
  { name: "node", start: startNode },
  { name: "python", start: startPython },
  { name: "rust", start: startRust },
  { name: "go", start: startGo },
  { name: "elixir", start: startElixir },
];

// Pre-build subprocess binaries so each test only pays the spawn cost.
export function ensureBuilt(): void {
  if (process.env.ELECTROLITE_MATRIX_SKIP_BUILD) return;
  const goBin = join(ROOT, "engines/go/server/server-bin");
  if (!existsSync(goBin)) {
    const r = spawnSync("go", ["build", "-o", goBin, "./server"], {
      cwd: join(ROOT, "engines/go"),
      stdio: "inherit",
    });
    if (r.status !== 0) throw new Error("go build failed");
  }
  const rustCandidates = [
    join(ROOT, "engines/rust/target/debug/electrolite-server"),
    join(ROOT, "target/debug/electrolite-server"),
  ];
  if (!rustCandidates.some(existsSync)) {
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
  // Elixir compile cache is per-project; first mix run compiles deps
  // then exits via timeout. Skip pre-warm; matrix tolerates the
  // first run being a few seconds slower.
}
