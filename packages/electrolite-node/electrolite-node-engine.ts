import { createHash, randomBytes } from "node:crypto";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

interface TableInfo {
  table: string;
  columns: string[];
  pkColumns: string[];
  columnTypes: Record<string, string>;
}

interface Shape {
  name?: string;
  table: string;
  columns: string[];
  predicate?: Predicate;
  auth_scope?: string;
  schema_version?: number;
}

type Predicate =
  | { type: "all" }
  | { type: "eq"; column: string; value: unknown }
  | { type: "in"; column: string; values: unknown[] }
  | { type: "and"; predicates: Predicate[] };

interface CompiledPredicate {
  where: string;
  params: unknown[];
}

interface LogRow {
  seq: number;
  batch_id: string;
  table_name: string;
  op: string;
  pk_json: Record<string, unknown>;
  old_pk_json: Record<string, unknown> | null;
  new_pk_json: Record<string, unknown> | null;
  old_json: Record<string, unknown> | null;
  new_json: Record<string, unknown> | null;
  created_at: number;
}

interface ShapeMessage {
  type: "insert" | "update" | "delete";
  batch_id: string;
  key: Record<string, unknown>;
  value?: Record<string, unknown>;
  offset: number;
}

export class JsElectroliteEngine {
  db;
  tableInfoCache: Map<string, TableInfo>;

  constructor(dbPath: string) {
    const { DatabaseSync } = loadNodeSqlite();
    this.db = new DatabaseSync(dbPath);
    this.tableInfoCache = new Map();
    this.bootstrap();
  }

  installTriggersAuto(table: string): string {
    this.tableInfoCache.delete(table);
    const info = this.inspectTable(table);
    if (info.pkColumns.length === 0) {
      throw new Error(`table ${table} must have a primary key`);
    }
    return this.installTriggersForInfo(info);
  }

  installTriggers(table: string, pkColumn: string): string {
    this.tableInfoCache.delete(table);
    const info = this.inspectTable(table);
    if (!info.columns.includes(pkColumn)) {
      throw new Error(`primary key column ${pkColumn} does not exist on ${table}`);
    }
    return this.installTriggersForInfo({ ...info, pkColumns: [pkColumn] });
  }

  installTriggersForInfo(info: TableInfo): string {
    this.bootstrap();
    this.db.prepare(`
      INSERT INTO _electrolite_watched_tables (table_name, pk_columns)
      VALUES (?, ?)
      ON CONFLICT(table_name) DO UPDATE SET pk_columns = excluded.pk_columns
    `).run(info.table, JSON.stringify(info.pkColumns));

    const tableName = quoteIdent(info.table);
    const tableLiteral = quoteString(info.table);
    const oldRow = rowJsonExpr("OLD", info.columns);
    const newRow = rowJsonExpr("NEW", info.columns);
    const oldPk = rowJsonExpr("OLD", info.pkColumns);
    const newPk = rowJsonExpr("NEW", info.pkColumns);
    const batchId = batchIdExpr();

    this.db.exec(`
      DROP TRIGGER IF EXISTS ${quoteIdent(`_electrolite_${info.table}_ai`)};
      DROP TRIGGER IF EXISTS ${quoteIdent(`_electrolite_${info.table}_au`)};
      DROP TRIGGER IF EXISTS ${quoteIdent(`_electrolite_${info.table}_ad`)};

      CREATE TRIGGER ${quoteIdent(`_electrolite_${info.table}_ai`)}
      AFTER INSERT ON ${tableName}
      BEGIN
        INSERT INTO _electrolite_log (
          batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
        )
        VALUES (${batchId}, ${tableLiteral}, 'insert', ${newPk}, NULL, ${newPk}, NULL, ${newRow});
      END;

      CREATE TRIGGER ${quoteIdent(`_electrolite_${info.table}_au`)}
      AFTER UPDATE ON ${tableName}
      BEGIN
        INSERT INTO _electrolite_log (
          batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
        )
        VALUES (${batchId}, ${tableLiteral}, 'update', ${newPk}, ${oldPk}, ${newPk}, ${oldRow}, ${newRow});
      END;

      CREATE TRIGGER ${quoteIdent(`_electrolite_${info.table}_ad`)}
      AFTER DELETE ON ${tableName}
      BEGIN
        INSERT INTO _electrolite_log (
          batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json
        )
        VALUES (${batchId}, ${tableLiteral}, 'delete', ${oldPk}, ${oldPk}, NULL, ${oldRow}, NULL);
      END;
    `);

    return JSON.stringify({
      table: info.table,
      key_columns: info.pkColumns,
      columns: info.columns,
    });
  }

  inspectTable(table: string): TableInfo {
    const cached = this.tableInfoCache.get(table);
    if (cached) {
      return {
        ...cached,
        columns: [...cached.columns],
        pkColumns: [...cached.pkColumns],
        columnTypes: { ...cached.columnTypes },
      };
    }
    const info = inspectTable(this.db, table);
    this.tableInfoCache.set(table, {
      ...info,
      columns: [...info.columns],
      pkColumns: [...info.pkColumns],
      columnTypes: { ...info.columnTypes },
    });
    return info;
  }

  snapshot(shapeJson: string): string {
    const shape = JSON.parse(shapeJson) as Shape;
    const info = this.validatedWatchedTable(shape);
    const normalizedShape = normalizeShape(info, shape);
    const compiled = compilePredicate(info, normalizedShape.predicate);
    return readTransaction(this.db, () => {
      const offset = this.highWaterMark();
      const sql = [
        `SELECT ${rowJsonExpr("", shape.columns)} AS row_json FROM ${quoteIdent(shape.table)}`,
        compiled.where ? `WHERE ${compiled.where}` : "",
        `ORDER BY ${info.pkColumns.map(quoteIdent).join(", ")}`,
      ].filter(Boolean).join(" ");
      const rows = this.db.prepare(sql).all(...compiled.params)
        .map((row) => JSON.parse(row.row_json));
      return JSON.stringify({
        type: "snapshot",
        log_id: logIdUnbootstrapped(this.db),
        shape_handle: shapeHandleFor(normalizedShape),
        key_columns: info.pkColumns,
        rows,
        offset,
        up_to_date: true,
      });
    });
  }

  replay(shapeJson: string, offset: number, limit = 1000): string {
    const shape = JSON.parse(shapeJson) as Shape;
    const info = this.validatedWatchedTable(shape);
    const normalizedShape = normalizeShape(info, shape);
    return readTransaction(this.db, () => {
      const retainedOffset = retainedOffsetForTable(this.db, shape.table);
      if (offset < retainedOffset) {
        throw new Error(`resync required: requested offset ${offset} is older than retained offset ${retainedOffset}`);
      }

      const page = readLogPage(this.db, shape.table, offset, Math.max(1, Number(limit)));
      const messages = [];
      let latest = offset;
      for (const row of page.rows) {
        latest = Math.max(latest, row.seq);
        messages.push(...messagesForLog(normalizedShape, row));
      }
      return JSON.stringify({
        type: "replay",
        log_id: logIdUnbootstrapped(this.db),
        shape_handle: shapeHandleFor(normalizedShape),
        messages,
        offset: latest,
        up_to_date: page.upToDate,
      });
    });
  }

  shapeHandle(shapeJson: string): string {
    const shape = JSON.parse(shapeJson) as Shape;
    const info = this.validatedWatchedTable(shape);
    return shapeHandleFor(normalizeShape(info, shape));
  }

  highWaterMark(): number {
    return this.db.prepare("SELECT COALESCE(MAX(seq), 0) AS seq FROM _electrolite_log").get().seq;
  }

  logId(): string {
    this.bootstrap();
    return this.db.prepare("SELECT value FROM _electrolite_meta WHERE key = 'log_id'").get().value;
  }

  compactLogToLastForTable(tableName: string, keepLast: number): string {
    const rows = this.db.prepare(`
      SELECT seq FROM _electrolite_log
      WHERE table_name = ?
      ORDER BY seq DESC
      LIMIT 1 OFFSET ?
    `).all(tableName, Math.max(0, Number(keepLast)));
    const retainedOffset = rows[0]?.seq ?? this.highWaterMark();
    const deleted = this.db.prepare(
      "DELETE FROM _electrolite_log WHERE table_name = ? AND seq <= ?",
    ).run(tableName, retainedOffset).changes;
    this.db.prepare(`
      INSERT INTO _electrolite_meta (key, value)
      VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `).run(`retained_offset:${tableName}`, String(retainedOffset));
    return JSON.stringify({ retained_offset: retainedOffset, deleted_rows: deleted });
  }

  executeBatch(sql: string): void {
    this.db.exec(sql);
  }

  execute(sql: string, paramsJson = "[]"): number {
    const params = JSON.parse(paramsJson || "[]");
    return this.db.prepare(normalizePlaceholders(sql)).run(...params).changes;
  }

  writeBatch(statementsJson: string): void {
    const statements = JSON.parse(statementsJson);
    const batchId = randomHex(16);
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db.prepare(`
        INSERT INTO _electrolite_meta (key, value)
        VALUES ('current_batch_id', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
      `).run(batchId);
      for (const statement of statements) {
        this.db.prepare(normalizePlaceholders(statement.sql)).run(...(statement.params ?? []));
      }
      this.db.prepare("DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'").run();
      this.db.exec("COMMIT");
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        // Ignore rollback failures so the original write error is preserved.
      }
      throw error;
    }
  }

  bootstrap(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS _electrolite_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _electrolite_watched_tables (
        table_name TEXT PRIMARY KEY,
        pk_columns TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _electrolite_log (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        table_name TEXT NOT NULL,
        op TEXT NOT NULL,
        pk_json TEXT NOT NULL,
        old_pk_json TEXT,
        new_pk_json TEXT,
        old_json TEXT,
        new_json TEXT,
        created_at INTEGER NOT NULL DEFAULT (unixepoch())
      );
      CREATE INDEX IF NOT EXISTS _electrolite_log_table_seq_idx
        ON _electrolite_log (table_name, seq);
      CREATE INDEX IF NOT EXISTS _electrolite_log_table_batch_seq_idx
        ON _electrolite_log (table_name, batch_id, seq);
    `);
    this.db.prepare(`
      INSERT INTO _electrolite_meta (key, value)
      SELECT 'log_id', ?
      WHERE NOT EXISTS (SELECT 1 FROM _electrolite_meta WHERE key = 'log_id')
    `).run(randomHex(16));
    // A crashed write_batch may have left current_batch_id behind;
    // clear it so the next unrelated write does not inherit a dead
    // batch_id.
    this.db.prepare(
      "DELETE FROM _electrolite_meta WHERE key = 'current_batch_id'",
    ).run();
  }

  validatedWatchedTable(shape: Shape): TableInfo {
    const info = this.inspectTable(shape.table);
    const watched = this.db.prepare(
      "SELECT pk_columns FROM _electrolite_watched_tables WHERE table_name = ?",
    ).get(shape.table);
    if (!watched) {
      throw new Error(`table ${shape.table} is not watched by Electrolite`);
    }
    info.pkColumns = JSON.parse(watched.pk_columns);
    for (const column of shape.columns) {
      if (!info.columns.includes(column)) {
        throw new Error(`shape column ${column} does not exist on ${shape.table}`);
      }
    }
    for (const column of info.pkColumns) {
      if (!shape.columns.includes(column)) {
        throw new Error(`shape columns must include primary key column ${column}`);
      }
    }
    return info;
  }
}

function inspectTable(db, table) {
  const rows = db.prepare(`PRAGMA table_info(${quoteString(table)})`).all();
  if (rows.length === 0) {
    throw new Error(`table ${table} does not exist`);
  }
  rows.sort((a, b) => a.cid - b.cid);
  return {
    table,
    columns: rows.map((row) => row.name),
    pkColumns: rows.filter((row) => row.pk > 0).sort((a, b) => a.pk - b.pk).map((row) => row.name),
    columnTypes: Object.fromEntries(rows.map((row) => [row.name, String(row.type ?? "")])),
  };
}

function rowJsonExpr(prefix, columns) {
  const qualifier = prefix ? `${prefix}.` : "";
  return `json_object(${columns
    .map((column) => `${quoteString(column)}, ${qualifier}${quoteIdent(column)}`)
    .join(", ")})`;
}

function batchIdExpr() {
  return "COALESCE((SELECT value FROM _electrolite_meta WHERE key = 'current_batch_id'), lower(hex(randomblob(16))))";
}

function retainedOffsetForTable(db, tableName) {
  const row = db.prepare("SELECT value FROM _electrolite_meta WHERE key = ?")
    .get(`retained_offset:${tableName}`);
  return row ? Number(row.value) : 0;
}

function readLogPage(db, tableName, offset, limit) {
  const rows = db.prepare(`
    SELECT seq, batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json, created_at
    FROM _electrolite_log
    WHERE table_name = ? AND seq > ?
    ORDER BY seq
    LIMIT ?
  `).all(tableName, offset, limit).map(parseLogRow);
  if (rows.length > 0) {
    const last = rows[rows.length - 1];
    // Extend until the trailing batch finishes or until we hit the
    // safety cap. Without the cap, a 10M-row batch would force
    // replay to load every row in one response.
    const extensionCap = Math.max(limit, 1) * 10;
    const rest = db.prepare(`
      SELECT seq, batch_id, table_name, op, pk_json, old_pk_json, new_pk_json, old_json, new_json, created_at
      FROM _electrolite_log
      WHERE table_name = ? AND seq > ? AND batch_id = ?
      ORDER BY seq
      LIMIT ?
    `).all(tableName, last.seq, last.batch_id, extensionCap).map(parseLogRow);
    rows.push(...rest);
  }
  const latest = rows.at(-1)?.seq ?? offset;
  const newer = db.prepare(
    "SELECT 1 AS found FROM _electrolite_log WHERE table_name = ? AND seq > ? LIMIT 1",
  ).get(tableName, latest);
  return { rows, upToDate: !newer };
}

function readTransaction(db, fn) {
  db.exec("BEGIN DEFERRED");
  try {
    const value = fn();
    db.exec("COMMIT");
    return value;
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // Preserve the original read error.
    }
    throw error;
  }
}

function parseLogRow(row) {
  return {
    seq: row.seq,
    batch_id: row.batch_id,
    table_name: row.table_name,
    op: row.op,
    pk_json: JSON.parse(row.pk_json),
    old_pk_json: row.old_pk_json ? JSON.parse(row.old_pk_json) : null,
    new_pk_json: row.new_pk_json ? JSON.parse(row.new_pk_json) : null,
    old_json: row.old_json ? JSON.parse(row.old_json) : null,
    new_json: row.new_json ? JSON.parse(row.new_json) : null,
    created_at: row.created_at,
  };
}

function logIdUnbootstrapped(db) {
  return db.prepare("SELECT value FROM _electrolite_meta WHERE key = 'log_id'").get().value;
}

function messagesForLog(shape, row) {
  if (row.table_name !== shape.table) {
    return [];
  }
  const oldMatches = predicateMatches(shape.predicate, row.old_json);
  const newMatches = predicateMatches(shape.predicate, row.new_json);
  const oldKey = row.old_pk_json ?? row.pk_json;
  const newKey = row.new_pk_json ?? row.pk_json;

  if (!oldMatches && newMatches && row.new_json) {
    return [{ type: "insert", batch_id: row.batch_id, key: newKey, value: row.new_json, offset: row.seq }];
  }
  if (oldMatches && newMatches && row.new_json) {
    if (JSON.stringify(oldKey) === JSON.stringify(newKey)) {
      return [{ type: "update", batch_id: row.batch_id, key: newKey, value: row.new_json, offset: row.seq }];
    }
    return [
      { type: "delete", batch_id: row.batch_id, key: oldKey, offset: row.seq },
      { type: "insert", batch_id: row.batch_id, key: newKey, value: row.new_json, offset: row.seq },
    ];
  }
  if (oldMatches && !newMatches) {
    return [{ type: "delete", batch_id: row.batch_id, key: oldKey, offset: row.seq }];
  }
  return [];
}

const RANGE_OPS = { gt: ">", lt: "<", gte: ">=", lte: "<=" };

function compilePredicate(info, predicate) {
  switch (predicate?.type) {
    case "all":
    case undefined:
      return { where: "", params: [] };
    case "eq": {
      const value = normalizePredicateValue(info, predicate.column, predicate.value);
      if (value === null) {
        return { where: `${quoteIdent(predicate.column)} IS NULL`, params: [] };
      }
      return { where: `${quoteIdent(predicate.column)} = ?`, params: [value] };
    }
    case "gt":
    case "lt":
    case "gte":
    case "lte": {
      const value = normalizePredicateValue(info, predicate.column, predicate.value);
      if (value === null) {
        throw badInputError(`range predicate ${predicate.type} requires a non-null value`);
      }
      return { where: `${quoteIdent(predicate.column)} ${RANGE_OPS[predicate.type]} ?`, params: [value] };
    }
    case "in": {
      const values = predicate.values.map((value) => normalizePredicateValue(info, predicate.column, value));
      const nonNullValues = values.filter((value) => value !== null);
      const hasNull = nonNullValues.length !== values.length;
      if (nonNullValues.length === 0 && !hasNull) {
        return { where: "0", params: [] };
      }
      const parts = [];
      if (nonNullValues.length > 0) {
        parts.push(`${quoteIdent(predicate.column)} IN (${nonNullValues.map(() => "?").join(", ")})`);
      }
      if (hasNull) {
        parts.push(`${quoteIdent(predicate.column)} IS NULL`);
      }
      return {
        where: parts.map((part) => `(${part})`).join(" OR "),
        params: nonNullValues,
      };
    }
    case "and": {
      const compiled = predicate.predicates.map((child) => compilePredicate(info, child))
        .filter((child) => child.where);
      return {
        where: compiled.map((child) => `(${child.where})`).join(" AND "),
        params: compiled.flatMap((child) => child.params),
      };
    }
    default:
      throw new Error(`unsupported predicate type ${predicate?.type}`);
  }
}

function predicateMatches(predicate, row) {
  if (!row) {
    return false;
  }
  switch (predicate?.type) {
    case "all":
    case undefined:
      return true;
    case "eq":
      return sameJsonValue(row[predicate.column], predicate.value);
    case "gt":
    case "lt":
    case "gte":
    case "lte":
      return compareScalar(row[predicate.column], predicate.value, predicate.type);
    case "in":
      return predicate.values.some((value) => sameJsonValue(row[predicate.column], value));
    case "and":
      return predicate.predicates.every((child) => predicateMatches(child, row));
    default:
      throw new Error(`unsupported predicate type ${predicate?.type}`);
  }
}

// Public predicate matcher exposed for conformance testing. Same
// semantics as the internal one used during replay.
export function predicateMatchesRow(predicate, row) {
  return predicateMatches(predicate, row);
}

function compareScalar(left, right, op) {
  if (left === null || left === undefined || right === null || right === undefined) {
    return false;
  }
  if (typeof left !== typeof right) {
    return false;
  }
  switch (op) {
    case "gt": return left > right;
    case "lt": return left < right;
    case "gte": return left >= right;
    case "lte": return left <= right;
  }
  return false;
}

function normalizeShape(info, shape) {
  return canonicalShape({
    ...shape,
    columns: [...shape.columns].sort(),
    predicate: normalizePredicate(info, shape.predicate ?? { type: "all" }),
  });
}

function normalizePredicate(info, predicate) {
  switch (predicate?.type) {
    case "all":
    case undefined:
      return { type: "all" };
    case "eq":
      return {
        type: "eq",
        column: predicate.column,
        value: normalizePredicateValue(info, predicate.column, predicate.value),
      };
    case "gt":
    case "lt":
    case "gte":
    case "lte":
      return {
        type: predicate.type,
        column: predicate.column,
        value: normalizePredicateValue(info, predicate.column, predicate.value),
      };
    case "in":
      return {
        type: "in",
        column: predicate.column,
        values: [...new Set(predicate.values.map((value) => JSON.stringify(
          normalizePredicateValue(info, predicate.column, value),
        )))].map(JSON.parse).sort(compareJson),
      };
    case "and":
      // Sort children by canonical (sorted-keys) JSON, not raw
      // JSON.stringify, so that insertion order in predicate
      // objects can't change child order. Other engines do the
      // same; without this, Node's `and` shape_handle diverges.
      return {
        type: "and",
        predicates: predicate.predicates.map((child) => normalizePredicate(info, child))
          .sort((a, b) => canonicalJson(a).localeCompare(canonicalJson(b))),
      };
    default:
      throw new Error(`unsupported predicate type ${predicate?.type}`);
  }
}

function normalizePredicateValue(info, column, value) {
  if (!info.columns.includes(column)) {
    throw badInputError(`predicate column ${column} does not exist on ${info.table}`);
  }
  const columnType = columnTypeInfo(info.columnTypes[column]);
  if (value === null) {
    return null;
  }
  if (typeof value === "boolean" && columnType.booleanish) {
    return value ? 1 : 0;
  }
  if (typeof value === "boolean") {
    throw unsupportedPredicateValue(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw unsupportedPredicateValue(value);
    }
    if (columnType.affinity === "integer" || columnType.affinity === "numeric") {
      if (!Number.isInteger(value)) {
        throw unsupportedPredicateValue(value);
      }
      return value;
    }
    if (columnType.affinity === "real") {
      return value;
    }
    if (columnType.affinity === "blob" && columnType.declaredType.trim() === "") {
      return value;
    }
    throw unsupportedPredicateValue(value);
  }
  if (typeof value === "string") {
    if (columnType.affinity === "text" || (columnType.affinity === "blob" && columnType.declaredType.trim() === "")) {
      return value;
    }
    throw unsupportedPredicateValue(value);
  }
  throw unsupportedPredicateValue(value);
}

function unsupportedPredicateValue(value) {
  const err = new Error(`unsupported predicate value ${JSON.stringify(value)}`);
  // Tag so handle() can map to 400 rather than 500.
  err.electroliteBadInput = true;
  return err;
}

function badInputError(message) {
  const err = new Error(message);
  err.electroliteBadInput = true;
  return err;
}

function isBooleanish(type) {
  return /\bBOOL(EAN)?\b/i.test(type);
}

function columnTypeInfo(declaredType = "") {
  const upper = String(declaredType).toUpperCase();
  let affinity;
  if (upper.includes("INT")) {
    affinity = "integer";
  } else if (upper.includes("CHAR") || upper.includes("CLOB") || upper.includes("TEXT")) {
    affinity = "text";
  } else if (upper.includes("BLOB") || upper.trim() === "") {
    affinity = "blob";
  } else if (upper.includes("REAL") || upper.includes("FLOA") || upper.includes("DOUB")) {
    affinity = "real";
  } else {
    affinity = "numeric";
  }
  return {
    declaredType: String(declaredType),
    affinity,
    booleanish: isBooleanish(declaredType),
  };
}

function canonicalShape(shape) {
  return {
    table: shape.table,
    columns: shape.columns,
    predicate: shape.predicate,
    auth_scope: shape.auth_scope ?? "",
    schema_version: shape.schema_version ?? 1,
  };
}

function shapeHandleFor(shape) {
  return createHash("sha256").update(canonicalJson(canonicalShape(shape))).digest("hex");
}

// Canonical JSON: sorted keys at every nesting level, no whitespace.
// Must produce identical bytes to Python's json.dumps(sort_keys=True,
// separators=(",", ":")) and to the Rust/Go/Elixir canonical encoders.
function canonicalJson(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return "[" + value.map(canonicalJson).join(",") + "]";
  }
  const keys = Object.keys(value).sort();
  return (
    "{" +
    keys
      .map((key) => JSON.stringify(key) + ":" + canonicalJson(value[key]))
      .join(",") +
    "}"
  );
}

function sameJsonValue(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function compareJson(left, right) {
  return JSON.stringify(left).localeCompare(JSON.stringify(right));
}

function quoteIdent(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function quoteString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function randomHex(bytes) {
  return randomBytes(bytes).toString("hex");
}

function normalizePlaceholders(sql) {
  return String(sql).replace(/\?\d+/g, "?");
}

function loadNodeSqlite() {
  try {
    return require("node:sqlite");
  } catch (error) {
    error.message = [
      "Electrolite requires Node's built-in node:sqlite module.",
      "Use Node 24+ to run Electrolite.",
      `Original error: ${error.message}`,
    ].join("\n");
    throw error;
  }
}
