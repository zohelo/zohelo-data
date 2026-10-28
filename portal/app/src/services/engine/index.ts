/**
 * Data engine — the transport-independent execution layer.
 *
 * Import from here rather than reaching into the submodules. See
 * `docs/architecture/execution.md` for the design and its constraints.
 */

export type { DataSession, QueryExecution } from "./types";

export { collectExecution, materializeCollected, runQuery } from "./queryStream";

export { asLocalDuckSession, requireLocalDuckSession, type LocalDuckSession } from "./session";

export {
  closeAllSessions,
  closeSession,
  getSession,
  listSessions,
  openSession,
  testConnection,
} from "./registry";

export { builtInWasmConnection, WASM_CONNECTION_ID } from "./drivers/wasmDriver";
export { catalogToDatabaseInfo, toConnectionDefinition, toCredentialMaterial } from "./legacy";
