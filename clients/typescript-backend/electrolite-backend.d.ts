export type ElectroliteRouteKind = "shape" | "factory";

export type ElectrolitePredicate =
  | { type: "all" }
  | { type: "eq"; column: string; value: unknown }
  | { type: "in"; column: string; values: unknown[] }
  | { type: "and"; predicates: ElectrolitePredicate[] };

export interface ElectroliteShapeSpec {
  name: string;
  table: string;
  columns: string[];
  predicate: ElectrolitePredicate;
  auth_scope: string;
  schema_version: number;
}

export interface ElectroliteRoute {
  kind: ElectroliteRouteKind;
  name: string;
  path: string;
  offset: number;
  live: boolean;
  url: URL;
  forwardPath: string;
}

export interface ElectroliteProxyContext extends ElectroliteRoute {
  request: Request;
}

export type ElectroliteAuthorizationDecision =
  | boolean
  | {
      allow: boolean;
      headers?: HeadersInit;
    };

export type ElectroliteAuthorize = (
  context: ElectroliteProxyContext,
) =>
  | ElectroliteAuthorizationDecision
  | Promise<ElectroliteAuthorizationDecision>;

export interface ElectroliteProxyOptions {
  origin: string | URL;
  prefix?: string;
  authorize: ElectroliteAuthorize;
  fetch?: typeof fetch;
}

export declare function createElectroliteProxy(
  options: ElectroliteProxyOptions,
): (request: Request) => Promise<Response>;

export declare function trustedShapeHeaders(
  shape: ElectroliteShapeSpec,
): Record<string, string>;

export declare function parseElectroliteRequest(
  url: string | URL,
  options?: { prefix?: string },
): ElectroliteRoute | null;
