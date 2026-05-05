export type ElectrolitePredicate =
  | { type: "all" }
  | { type: "eq"; column: string; value: unknown }
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

export class Electrolite<TContext = unknown> {
  constructor(options: ElectroliteOptions<TContext>);
  installTriggers(table: string): InstallResult;
  installTriggersFor(table: string, pkColumn: string): InstallResult;
  executeBatch(sql: string): void;
  execute(sql: string, params?: unknown[]): number;
  writeBatch(statements: Array<[sql: string, params?: unknown[]]>): void;
  compactLogToLastForTable(tableName: string, keepLast: number): RetentionStats;
  notifyChanged(): void;
  fetch(request: Request, context?: TContext): Promise<Response>;
  handle(request: Request, context?: TContext): Promise<Response>;
}

export declare function createElectrolite<TContext = unknown>(
  options: ElectroliteOptions<TContext>,
): Electrolite<TContext>;

export declare function shape<TContext = unknown>(
  definition: ShapeDefinition<TContext>,
): ShapeDefinition<TContext>;

export declare const all: () => ElectrolitePredicate;
export declare const eq: (column: string, value: unknown) => ElectrolitePredicate;
export declare const inList: (
  column: string,
  values: unknown[],
) => ElectrolitePredicate;
export declare const and: (
  predicates: ElectrolitePredicate[],
) => ElectrolitePredicate;

