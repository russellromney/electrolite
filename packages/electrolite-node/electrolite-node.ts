import { JsElectroliteEngine } from "./electrolite-node-engine.ts";

export type ElectrolitePredicate =
  | { type: "all" }
  | { type: "eq"; column: string; value: unknown }
  | { type: "gt"; column: string; value: unknown }
  | { type: "lt"; column: string; value: unknown }
  | { type: "gte"; column: string; value: unknown }
  | { type: "lte"; column: string; value: unknown }
  | { type: "in"; column: string; values: unknown[] }
  | { type: "and"; predicates: ElectrolitePredicate[] };

export interface ShapeDefinitionContext<TContext = unknown> {
  params: Record<string, string>;
  request: Request;
  context: TContext;
}

export interface ShapeAuthorizeContext<TContext = unknown>
  extends ShapeDefinitionContext<TContext> {
  scope: string;
}

export interface ShapeDefinition<TContext = unknown> {
  table: string;
  columns: string[];
  params?: string[];
  where?: (
    context: ShapeDefinitionContext<TContext>,
  ) => ElectrolitePredicate | Promise<ElectrolitePredicate>;
  scope?:
    | string
    | ((
        context: ShapeDefinitionContext<TContext>,
      ) => string | Promise<string>);
  authorize?: (
    context: ShapeAuthorizeContext<TContext>,
  ) => boolean | Promise<boolean>;
  schemaVersion?: number;
}

export interface ElectroliteOptions<TContext = unknown> {
  dbPath: string;
  shapes?: Record<string, ShapeDefinition<TContext>>;
  prefix?: string;
  replayLimit?: number;
  liveTimeoutMs?: number;
  pollIntervalMs?: number;
}

export interface InstallResult {
  table: string;
  key_columns: string[];
  columns: string[];
}

export interface RetentionStats {
  retained_offset: number;
  deleted_rows: number;
}

interface BuiltShape {
  allow: boolean;
  shape?: {
    name: string;
    table: string;
    columns: string[];
    predicate: ElectrolitePredicate;
    auth_scope: string;
    schema_version: number;
  };
}

type Waiter = () => void;

export const all = (): ElectrolitePredicate => ({ type: "all" });
export const eq = (column: string, value: unknown): ElectrolitePredicate => ({ type: "eq", column, value });
export const gt = (column: string, value: unknown): ElectrolitePredicate => ({ type: "gt", column, value });
export const lt = (column: string, value: unknown): ElectrolitePredicate => ({ type: "lt", column, value });
export const gte = (column: string, value: unknown): ElectrolitePredicate => ({ type: "gte", column, value });
export const lte = (column: string, value: unknown): ElectrolitePredicate => ({ type: "lte", column, value });
export const inList = (column: string, values: unknown[]): ElectrolitePredicate => ({ type: "in", column, values });
export const and = (predicates: ElectrolitePredicate[]): ElectrolitePredicate => ({ type: "and", predicates });

export function shape<TContext = unknown>(
  definition: ShapeDefinition<TContext>,
): ShapeDefinition<TContext> {
  return { ...definition };
}

export function createElectrolite<TContext = unknown>(
  options: ElectroliteOptions<TContext>,
): Electrolite<TContext> {
  return new Electrolite<TContext>(options);
}

export class Electrolite<TContext = unknown> {
  engine: JsElectroliteEngine;
  shapes: Record<string, ShapeDefinition<TContext>>;
  prefix: string;
  replayLimit: number;
  liveTimeoutMs: number;
  pollIntervalMs: number;
  activeShapes: Map<string, BuiltShape["shape"]>;
  waiters: Map<string, Set<Waiter>>;

  constructor(options: ElectroliteOptions<TContext>) {
    const {
      dbPath,
      shapes = {},
      prefix = "/electrolite/v1",
      replayLimit = 1000,
      liveTimeoutMs = 20_000,
      pollIntervalMs = 250,
    } = options;
    if (!dbPath) {
      throw new Error("createElectrolite requires dbPath");
    }

    this.engine = new JsElectroliteEngine(String(dbPath));
    this.shapes = shapes;
    this.prefix = normalizePrefix(prefix);
    this.replayLimit = replayLimit;
    this.liveTimeoutMs = liveTimeoutMs;
    this.pollIntervalMs = pollIntervalMs;
    this.activeShapes = new Map();
    this.waiters = new Map();
  }

  installTriggers(table: string): InstallResult {
    return JSON.parse(this.engine.installTriggersAuto(String(table)));
  }

  installTriggersFor(table: string, pkColumn: string): InstallResult {
    return JSON.parse(this.engine.installTriggers(String(table), String(pkColumn)));
  }

  executeBatch(sql: string): void {
    const offset = this.highWaterMark();
    this.engine.executeBatch(String(sql));
    this.notifyChangedFrom(offset);
  }

  execute(sql: string, params: unknown[] = []): number {
    const offset = this.highWaterMark();
    const rows = this.engine.execute(String(sql), JSON.stringify(params));
    this.notifyChangedFrom(offset);
    return rows;
  }

  writeBatch(statements: Array<[sql: string, params?: unknown[]]>): void {
    const offset = this.highWaterMark();
    const normalized = statements.map(([sql, params = []]) => ({ sql, params }));
    this.engine.writeBatch(JSON.stringify(normalized));
    this.notifyChangedFrom(offset);
  }

  highWaterMark(): number {
    return this.engine.highWaterMark();
  }

  logId(): string {
    return this.engine.logId();
  }

  compactLogToLastForTable(tableName: string, keepLast: number): RetentionStats {
    return JSON.parse(
      this.engine.compactLogToLastForTable(String(tableName), Number(keepLast)),
    );
  }

  notifyChanged(): void {
    const waiters = new Set();
    for (const shapeWaiters of this.waiters.values()) {
      for (const waiter of shapeWaiters) {
        waiters.add(waiter);
      }
    }
    this.waiters.clear();
    for (const waiter of waiters) {
      waiter();
    }
  }

  notifyChangedFrom(offset: number): void {
    if (this.activeShapes.size === 0) {
      return;
    }

    const nextOffset = this.highWaterMark();
    if (nextOffset <= offset) {
      return;
    }

    const limit = Math.max(1, nextOffset - offset);
    for (const [shapeHandle, shape] of this.activeShapes) {
      try {
        const replay = JSON.parse(
          this.engine.replay(JSON.stringify(shape), Number(offset), limit),
        );
        if (replay.messages.length > 0) {
          this.notifyShapeChanged(shapeHandle);
        }
      } catch {
        this.notifyShapeChanged(shapeHandle);
      }
    }
  }

  notifyShapeChanged(shapeHandle: string): void {
    const waiters = this.waiters.get(shapeHandle);
    if (!waiters) {
      return;
    }
    this.waiters.delete(shapeHandle);
    for (const waiter of waiters) {
      waiter();
    }
  }

  async fetch(request: Request, context?: TContext): Promise<Response> {
    return this.handle(request, context);
  }

  async handle(request: Request, context?: TContext): Promise<Response> {
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

    if (route.offset >= 0 && route.logId && route.logId !== this.logId()) {
      return jsonError(409, "resync_required");
    }

    if (route.offset < 0) {
      try {
        return jsonResponse(JSON.parse(this.engine.snapshot(JSON.stringify(built.shape))));
      } catch (error) {
        if (isBadInputError(error)) {
          return badInputResponse(error);
        }
        return jsonError(500, "internal_server_error");
      }
    }

    if (route.live) {
      return this.liveResponse(built.shape, route.offset);
    }

    return this.replayResponse(built.shape, route.offset);
  }

  async buildShape(
    definition: ShapeDefinition<TContext>,
    route,
    request: Request,
    context: TContext | undefined,
  ): Promise<BuiltShape> {
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
      return jsonResponse(
        JSON.parse(this.engine.replay(JSON.stringify(shape), Number(offset), this.replayLimit)),
      );
    } catch (error) {
      if (isResyncError(error)) {
        return jsonError(409, "resync_required");
      }
      if (isBadInputError(error)) {
        return badInputResponse(error);
      }
      return jsonError(500, "internal_server_error");
    }
  }

  async liveResponse(shape, offset) {
    const shapeHandle = this.engine.shapeHandle(JSON.stringify(shape));
    this.activeShapes.set(shapeHandle, shape);
    try {
      const deadline = Date.now() + this.liveTimeoutMs;
      while (Date.now() <= deadline) {
        let replay;
        try {
          replay = JSON.parse(
            this.engine.replay(JSON.stringify(shape), Number(offset), this.replayLimit),
          );
        } catch (error) {
          if (isResyncError(error)) {
            return jsonError(409, "resync_required");
          }
          if (isBadInputError(error)) {
            return badInputResponse(error);
          }
          return jsonError(500, "internal_server_error");
        }
        if (replay.offset > offset || replay.messages.length > 0) {
          return jsonResponse({
            type: "replay",
            ...replay,
            shape_handle: shapeHandle,
          });
        }

        const waiter = this.nextShapeChange(shapeHandle);
        try {
          await Promise.race([
            waiter.promise,
            sleep(Math.min(this.pollIntervalMs, Math.max(0, deadline - Date.now()))),
          ]);
        } finally {
          waiter.cancel();
        }
      }
      return new Response(null, { status: 204 });
    } finally {
      if (!this.waiters.has(shapeHandle)) {
        this.activeShapes.delete(shapeHandle);
      }
    }
  }

  nextShapeChange(shapeHandle) {
    let waiters = this.waiters.get(shapeHandle);
    if (!waiters) {
      waiters = new Set();
      this.waiters.set(shapeHandle, waiters);
    }
    let resolve;
    const promise = new Promise((done) => {
      resolve = done;
    });
    waiters.add(resolve);
    return {
      promise,
      cancel: () => {
        waiters.delete(resolve);
        if (waiters.size === 0 && this.waiters.get(shapeHandle) === waiters) {
          this.waiters.delete(shapeHandle);
        }
      },
    };
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
      logId: parsed.searchParams.get("log_id"),
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

function isBadInputError(error) {
  return Boolean(error?.electroliteBadInput);
}

function badInputResponse(error) {
  return Response.json(
    { error: "bad_request", detail: String(error?.message ?? error) },
    { status: 400 },
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
