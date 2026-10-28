/**
 * Bridge between the engine layer and the shapes the rest of Duck-UI still
 * speaks: `ConnectionProvider`/`CurrentConnection` with their
 * `"WASM" | "External" | "OPFS"` scope, and `DatabaseInfo[]` for the explorer.
 *
 * This file is the ONLY place allowed to know that mapping. Everything else
 * works in `ConnectionKind` and capabilities. It shrinks as consumers migrate
 * and disappears once persistence stores `kind` directly.
 */

import type { ConnectionProvider, CurrentConnection, DatabaseInfo, TableInfo } from "@/store/types";
import type {
  CatalogSnapshot,
  ConnectionDefinition,
  ConnectionKind,
  ConnectionOrigin,
  CredentialMaterial,
  HttpAuthMode,
} from "./types";

/** Legacy scope → engine kind. Unknown scopes fall back to the HTTP driver. */
const scopeToKind = (scope: string | undefined): ConnectionKind => {
  switch (scope) {
    case "WASM":
      return "wasm";
    case "OPFS":
      return "opfs";
    case "External":
      return "duck-http";
    default:
      return "duck-http";
  }
};

const toAuthMode = (
  value: string | undefined,
  hasApiKey: boolean,
  hasUser: boolean
): HttpAuthMode => {
  if (value === "api_key" || value === "password" || value === "none") return value;
  if (hasApiKey) return "api_key";
  if (hasUser) return "password";
  return "none";
};

/**
 * Builds an engine connection definition from a legacy provider record.
 * Secrets are deliberately dropped here — pull them with
 * `toCredentialMaterial` and keep them out of anything persisted or shared.
 */
export const toConnectionDefinition = (
  provider: ConnectionProvider | CurrentConnection
): ConnectionDefinition => {
  const id = provider.id;
  const name = provider.name;
  const origin: ConnectionOrigin = provider.environment ?? "APP";
  const kind = scopeToKind(provider.scope);

  switch (kind) {
    case "wasm":
      return { id, name, origin, config: { kind: "wasm" } };
    case "opfs":
      return { id, name, origin, config: { kind: "opfs", path: provider.path ?? "" } };
    default:
      return {
        id,
        name,
        origin,
        config: {
          kind: "duck-http",
          host: provider.host ?? "",
          port: provider.port,
          database: provider.database,
          user: provider.user,
          authMode: toAuthMode(provider.authMode, Boolean(provider.apiKey), Boolean(provider.user)),
        },
      };
  }
};

/** Extracts the secrets from a legacy provider record. */
export const toCredentialMaterial = (
  provider: ConnectionProvider | CurrentConnection
): CredentialMaterial => ({
  password: provider.password,
  apiKey: provider.apiKey,
});

/** Renders a catalog snapshot in the shape the data explorer consumes. */
export const catalogToDatabaseInfo = (snapshot: CatalogSnapshot): DatabaseInfo[] =>
  snapshot.databases.map((database) => ({
    name: database.name,
    tables: database.tables.map((table): TableInfo => ({
      name: table.name,
      schema: table.schema,
      relationType: table.relationType,
      columns: table.columns,
      rowCount: table.rowCount,
      createdAt: snapshot.capturedAt,
    })),
  }));
