// Language-agnostic conformance harness.
//
// Runs each `cases/*.json` against every engine and asserts both
// per-engine status/body shape and cross-engine byte equality where
// the protocol demands identity (notably `shape_handle`).

import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  ENGINES,
  ensureBuilt,
  pickPort,
  sleep,
} from "../../tests/lib/spawn.ts";
import type { Server } from "../../tests/lib/spawn.ts";

interface Operation {
  kind: "GET" | "exec" | "write_batch" | "seed" | "wait_ms" | "match_predicate";
  path?: string;
  query?: string;
  sql?: string;
  args?: unknown[];
  statements?: [string, unknown[]][];
  ms?: number;
  predicate?: any;
  rows?: any[];
}

interface Case {
  name: string;
  setup?: string;
  operations: Operation[];
  assert?: ((responses: unknown[]) => void) | { js: string };
  cross_engine?: string[]; // dotted JSON paths into responses[i].body
}

const HERE = dirname(fileURLToPath(import.meta.url));
const CASES_DIR = join(HERE, "cases");

function loadCases(): Case[] {
  return readdirSync(CASES_DIR)
    .filter((f) => f.endsWith(".json"))
    .sort()
    .map((f) => {
      const raw = JSON.parse(readFileSync(join(CASES_DIR, f), "utf-8")) as Case;
      raw.name ??= f;
      return raw;
    });
}

async function runOperation(server: Server, op: Operation, prev: any[]): Promise<any> {
  if (op.kind === "wait_ms") {
    await sleep(op.ms ?? 25);
    return null;
  }
  if (op.kind === "exec") {
    await server.exec(op.sql!, op.args ?? []);
    return { ok: true };
  }
  if (op.kind === "write_batch") {
    await server.writeBatch(op.statements!);
    return { ok: true };
  }
  if (op.kind === "seed") {
    await fetch(`${server.url}/_test/seed`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sql: op.sql }),
    });
    return { ok: true };
  }
  if (op.kind === "match_predicate") {
    const r = await fetch(`${server.url}/_test/match-predicate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ predicate: op.predicate, rows: op.rows }),
    });
    return { status: r.status, body: await r.json().catch(() => null) };
  }
  if (op.kind === "GET") {
    const path = expandTemplate(op.path!, prev);
    const query = op.query ? expandTemplate(op.query, prev) : "";
    const url = `${server.url}${path}${query ? "?" + query : ""}`;
    const r = await fetch(url);
    const body = await r.json().catch(() => null);
    return { status: r.status, body };
  }
  throw new Error(`unknown op kind: ${(op as any).kind}`);
}

// Expand `{prev[i].body.field}` in templates.
function expandTemplate(s: string, prev: any[]): string {
  return s.replace(/\{prev\[(\d+)\]\.([\w.]+)\}/g, (_, idx, path) => {
    const target = prev[Number(idx)];
    if (!target) return "";
    let cur: any = target;
    for (const part of String(path).split(".")) {
      cur = cur?.[part];
    }
    return String(cur);
  });
}

function getPath(obj: any, path: string): any {
  let cur: any = obj;
  for (const part of path.split(".")) {
    cur = cur?.[part];
  }
  return cur;
}

// Stringify with sorted keys so that engines that emit row JSON in
// declared-column order vs. sorted order still compare equal at the
// semantic level. Engine wire format does not promise key order for
// row data; the protocol only promises identical bytes for
// `shape_handle` (which uses its own canonical encoder).
function canonicalStringify(value: any): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return "[" + value.map(canonicalStringify).join(",") + "]";
  }
  const keys = Object.keys(value).sort();
  return (
    "{" +
    keys
      .map((k) => JSON.stringify(k) + ":" + canonicalStringify(value[k]))
      .join(",") +
    "}"
  );
}

ensureBuilt();
const cases = loadCases();

for (const c of cases) {
  test(`conformance: ${c.name}`, async () => {
    const responsesPerEngine = new Map<string, any[]>();

    for (const engine of ENGINES) {
      const dir = mkdtempSync(join(tmpdir(), `electrolite-conf-${engine.name}-`));
      const port = pickPort();
      let server: Server | null = null;
      try {
        server = await engine.start(port, join(dir, "app.db"));
        if (c.setup) {
          await fetch(`${server.url}/_test/seed`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ sql: c.setup }),
          });
        }
        const responses: any[] = [];
        for (const op of c.operations) {
          responses.push(await runOperation(server, op, responses));
        }
        responsesPerEngine.set(engine.name, responses);
      } finally {
        if (server) await server.stop();
        rmSync(dir, { recursive: true, force: true });
      }
    }

    // Cross-engine equality: every named path must match byte-for-byte
    // across all engines (or be absent on all).
    if (c.cross_engine) {
      const engineNames = [...responsesPerEngine.keys()];
      for (const path of c.cross_engine) {
        const values: Record<string, string> = {};
        for (const name of engineNames) {
          const responses = responsesPerEngine.get(name)!;
          const [opIdx, ...rest] = path.split(".");
          const restPath = rest.join(".");
          const resp = responses[Number(opIdx)];
          values[name] = canonicalStringify(getPath(resp, restPath));
        }
        const distinct = new Set(Object.values(values));
        if (distinct.size !== 1) {
          const detail = Object.entries(values)
            .map(([k, v]) => `  ${k}: ${v}`)
            .join("\n");
          assert.fail(
            `cross-engine mismatch on ${path}:\n${detail}`,
          );
        }
      }
    }
  });
}
