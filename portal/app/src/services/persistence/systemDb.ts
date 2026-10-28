/**
 * System Database Service
 *
 * Manages persistence for profiles, settings, connections, query history,
 * and other app state using IndexedDB.
 *
 * Previously attempted OPFS with a dedicated DuckDB WASM instance, but
 * IndexedDB is more appropriate for lightweight metadata and avoids
 * creating a heavyweight second DuckDB instance (worker + 34MB WASM).
 */

import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

/** Escape a string value for safe use in SQL single-quoted literals */
function sqlEscape(value: string): string {
  return value.replace(/'/g, "''");
}

/** Escape and quote a string value for use in SQL WHERE clauses etc. Returns 'escaped_value' */
export function sqlQuote(value: string): string {
  return `'${sqlEscape(value)}'`;
}

// Singleton state
let initialized = false;
let initPromise: Promise<void> | null = null;

/**
 * Initialize the system database. Sets up IndexedDB persistence.
 * Must be called before any repository functions.
 * Uses a Promise-based lock to prevent double-init races.
 */
export function initializeSystemDb(): Promise<void> {
  if (!initPromise) {
    initPromise = doInitialize();
  }
  return initPromise;
}

async function doInitialize(): Promise<void> {
  if (initialized) return;
  console.info("[SystemDB] Using IndexedDB persistence");
  initialized = true;
}

/**
 * Get the system database connection. Throws since system DB now uses IndexedDB.
 */
export function getSystemConnection(): AsyncDuckDBConnection {
  throw new Error("System database uses IndexedDB — no DuckDB connection available");
}

/**
 * Check if the system DB is using OPFS (vs IndexedDB fallback).
 * Always returns false since system DB now exclusively uses IndexedDB.
 */
export function isUsingOpfs(): boolean {
  return false;
}

/**
 * Check if system DB has been initialized.
 */
export function isSystemDbInitialized(): boolean {
  return initialized;
}
