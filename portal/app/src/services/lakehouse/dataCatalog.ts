/**
 * The platform data catalog (warehouse/catalog.py → _portal_catalog/v1/catalog.json),
 * served by the portal worker at /drive/v3/catalog.
 */
import { getStoredToken } from "./auth";
import { apiRequest, apiUrl } from "./api";

export interface CatalogColumn {
  name: string;
  description: string;
}

export interface CatalogTable {
  id: string;
  table: string;
  layer: string;
  description: string;
  columns: CatalogColumn[];
  upstream: string[];
  downstream: string[];
  tests: string[];
  path?: string;
}

export interface CatalogSource {
  id: string;
  table: string;
  source_system: string;
  description: string;
  downstream: string[];
  meta: Record<string, unknown>;
}

export interface CatalogMetric {
  name: string;
  label?: string | null;
  description?: string | null;
  type?: string | null;
}

export interface CatalogDimension {
  name: string;
  kind?: string | null;
  status?: string | null;
  description: string;
  grain?: string | null;
  used_by: string[];
  registry: string;
}

export interface DataCatalog {
  generated_at: string;
  about: string;
  counts: Record<string, number>;
  sources: CatalogSource[];
  tables: CatalogTable[];
  metrics: CatalogMetric[];
  dimensions: CatalogDimension[];
}

export const fetchDataCatalog = async (signal?: AbortSignal): Promise<DataCatalog> => {
  const token = getStoredToken();
  if (!token) throw new Error("Sign in to read the data catalog.");
  const response = await apiRequest(apiUrl("/drive/v3/catalog"), token, signal);
  return (await response.json()) as DataCatalog;
};
