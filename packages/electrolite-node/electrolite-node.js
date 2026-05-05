import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const native = require(`./native/electrolite_node_native.${process.platform}-${process.arch}.node`);

export const all = () => ({ type: "all" });
export const eq = (column, value) => ({ type: "eq", column, value });
export const inList = (column, values) => ({ type: "in", column, values });
export const and = (predicates) => ({ type: "and", predicates });

export function shape(definition) {
  return { ...definition };
}

export function createElectrolite(options) {
  return new Electrolite(options);
}

export class Electrolite {
  constructor(options = {}) {
    const {
      dbPath,
      shapes = {},
      prefix = "/electrolite/v1",
      replayLimit = 1000,
      liveTimeoutMs = 20_000,
      pollIntervalMs = 250,
      connectionPoolSize = 1,
    } = options;
    if (!dbPath) {
      throw new Error("createElectrolite requires dbPath");
    }

    this.native = new native.NativeElectrolite(
      String(dbPath),
      Math.max(1, Number(connectionPoolSize)),
    );
    this.shapes = shapes;
    this.prefix = normalizePrefix(prefix);
    this.replayLimit = replayLimit;
    this.liveTimeoutMs = liveTimeoutMs;
    this.pollIntervalMs = pollIntervalMs;
    this.waiters = new Set();
  }

  installTriggers(table) {
    return JSON.parse(this.native.installTriggersAuto(String(table)));
  }

  installTriggersFor(table, pkColumn) {
    return JSON.parse(this.native.installTriggers(String(table), String(pkColumn)));
  }

  executeBatch(sql) {
    this.native.executeBatch(String(sql));
    this.notifyChanged();
  }

  execute(sql, params = []) {
    const rows = this.native.execute(String(sql), JSON.stringify(params));
    this.notifyChanged();
    return rows;
  }

  writeBatch(statements) {
    const normalized = statements.map(([sql, params = []]) => ({ sql, params }));
    this.native.writeBatch(JSON.stringify(normalized));
    this.notifyChanged();
  }

  compactLogToLastForTable(tableName, keepLast) {
    return JSON.parse(
      this.native.compactLogToLastForTable(String(tableName), Number(keepLast)),
    );
  }

  notifyChanged() {
    for (const waiter of this.waiters) {
      waiter();
    }
    this.waiters.clear();
  }

  async fetch(request, context) {
    return this.handle(request, context);
  }

  async handle(request, context) {
    if (request.method !== "GET") {
      return jsonError(405, "method_not_allowed");
    }
    const route = this.parseRequest(request.url);
    if (!route) {
      return jsonError(404, "shape_not_found");
    }
    const definition = this.shapes[route.name];
    if (!definition) {
      return jsonError(404, "shape_not_found");
    }

    const built = await this.buildShape(definition, route, request, context);
    if (!built.allow) {
      return jsonError(404, "shape_not_found");
    }

    if (route.offset < 0) {
      return jsonResponse({
        type: "snapshot",
        ...JSON.parse(this.native.snapshot(JSON.stringify(built.shape))),
        shape_handle: this.native.shapeHandle(JSON.stringify(built.shape)),
      });
    }

    if (route.live) {
      return this.liveResponse(built.shape, route.offset);
    }

    return this.replayResponse(built.shape, route.offset);
  }

  async buildShape(definition, route, request, context) {
    const params = paramsFor(definition.params ?? [], route.path);
    if (!params) {
      return { allow: false };
    }

    const scope = callMaybe(definition.scope, { params, request, context });
    const authScope =
      scope === undefined || scope === null ? route.name : String(await scope);
    const authorized = definition.authorize
      ? await definition.authorize({ params, request, context, scope: authScope })
      : true;
    if (!authorized) {
      return { allow: false };
    }

    const predicate = definition.where
      ? await definition.where({ params, request, context })
      : all();
    return {
      allow: true,
      shape: {
        name: route.path.length > 0 ? `${route.name}/${route.path.join("/")}` : route.name,
        table: definition.table,
        columns: definition.columns,
        predicate,
        auth_scope: authScope,
        schema_version: definition.schemaVersion ?? 1,
      },
    };
  }

  replayResponse(shape, offset) {
    try {
      return jsonResponse({
        type: "replay",
        ...JSON.parse(
          this.native.replay(JSON.stringify(shape), Number(offset), this.replayLimit),
        ),
        shape_handle: this.native.shapeHandle(JSON.stringify(shape)),
      });
    } catch (error) {
      if (isResyncError(error)) {
        return jsonError(409, "resync_required");
      }
      return jsonError(500, "internal_server_error");
    }
  }

  async liveResponse(shape, offset) {
    const deadline = Date.now() + this.liveTimeoutMs;
    while (Date.now() <= deadline) {
      let replay;
      try {
        replay = JSON.parse(
          this.native.replay(JSON.stringify(shape), Number(offset), this.replayLimit),
        );
      } catch (error) {
        if (isResyncError(error)) {
          return jsonError(409, "resync_required");
        }
        return jsonError(500, "internal_server_error");
      }
      if (replay.offset > offset || replay.messages.length > 0) {
        return jsonResponse({
          type: "replay",
          ...replay,
          shape_handle: this.native.shapeHandle(JSON.stringify(shape)),
        });
      }

      await Promise.race([
        this.nextChange(),
        sleep(Math.min(this.pollIntervalMs, Math.max(0, deadline - Date.now()))),
      ]);
    }
    return new Response(null, { status: 204 });
  }

  nextChange() {
    return new Promise((resolve) => {
      this.waiters.add(resolve);
    });
  }

  parseRequest(url) {
    const parsed = new URL(url, "http://electrolite.local");
    if (!parsed.pathname.startsWith(`${this.prefix}/`)) {
      return null;
    }
    const tail = parsed.pathname.slice(this.prefix.length + 1);
    const segments = tail.split("/").filter(Boolean).map(safeDecode);
    if (segments.some((segment) => segment === null) || segments.length === 0) {
      return null;
    }
    const offset = Number(parsed.searchParams.get("offset"));
    if (!Number.isFinite(offset)) {
      return null;
    }
    return {
      name: segments[0],
      path: segments.slice(1),
      offset,
      live: parsed.searchParams.get("live") === "true",
    };
  }
}

function paramsFor(names, values) {
  if (names.length !== values.length) {
    return null;
  }
  return Object.fromEntries(names.map((name, index) => [name, values[index]]));
}

function callMaybe(fn, arg) {
  return typeof fn === "function" ? fn(arg) : fn;
}

function normalizePrefix(prefix) {
  const trimmed = String(prefix).replace(/^\/+|\/+$/g, "");
  return `/${trimmed}`;
}

function safeDecode(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

function jsonResponse(body) {
  return Response.json(body);
}

function jsonError(status, error) {
  return Response.json({ error }, { status });
}

function isResyncError(error) {
  const message = String(error?.message ?? error).toLowerCase();
  return (
    message.includes("resync")
    || message.includes("requested offset")
    || message.includes("older than retained offset")
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
