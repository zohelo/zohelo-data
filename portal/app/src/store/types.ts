import * as duckdb from "@duckdb/duckdb-wasm";
import type { DataSession, SessionCapabilities } from "@/services/engine/types";
import type {
  PortalAuthState,
  LandingCatalogResolution,
  LakehouseLayer,
  ReleaseCatalogResolution,
} from "@/services/lakehouse/types";
import type {
  NativeLandingFile,
  NativeLandingFolder,
  NativeMetadataProgress,
} from "@/services/lakehouse/nativeLandingCatalog";

//
// Global Window type augmentation
//

declare global {
  interface Window {
    env?: {
      DUCK_UI_EXTERNAL_CONNECTION_NAME: string;
      DUCK_UI_EXTERNAL_HOST: string;
      DUCK_UI_EXTERNAL_PORT: string;
      DUCK_UI_EXTERNAL_USER: string;
      DUCK_UI_EXTERNAL_PASS: string;
      DUCK_UI_EXTERNAL_API_KEY: string;
      DUCK_UI_EXTERNAL_DATABASE_NAME: string;
      DUCK_UI_ALLOW_UNSIGNED_EXTENSIONS: boolean;
      DUCK_UI_DUCKDB_WASM_USE_CDN?: boolean;
      DUCK_UI_DUCKDB_WASM_BASE_URL?: string;
    };
  }
}

//
// Connection Types
//

/** Where a connection came from. */
export type ConnectionEnvironment = "APP" | "ENV" | "BUILT_IN";

/** Connection kind. */
export type ConnectionScope = "WASM" | "External" | "OPFS";

export interface CurrentConnection {
  environment: ConnectionEnvironment;
  id: string;
  name: string;
  scope: ConnectionScope;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  authMode?: "none" | "password" | "api_key";
  apiKey?: string;
  path?: string;
}

export interface ConnectionProvider {
  environment: ConnectionEnvironment;
  id: string;
  name: string;
  scope: ConnectionScope;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  authMode?: "none" | "password" | "api_key";
  apiKey?: string;
  path?: string;
}

export interface ConnectionList {
  connections: ConnectionProvider[];
}

/**
 * Capabilities of the active session, or every flag off when nothing is
 * connected. UI code should branch on these rather than on a connection kind.
 */
export type ActiveCapabilities = SessionCapabilities;

//
// Database & Table Types
//

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
}

export interface ColumnStats {
  column_name: string;
  column_type: string;
  min: string | null;
  max: string | null;
  approx_unique: string | null;
  avg: string | null;
  std: string | null;
  q25: string | null;
  q50: string | null;
  q75: string | null;
  count: string;
  null_percentage: string;
}

/**
 * Lazily-fetched value distribution for one column: an equi-width histogram
 * for numeric columns, the top values by count for everything else.
 */
export type ColumnDistribution =
  | { kind: "histogram"; bins: number[] }
  | { kind: "topk"; values: { value: string; count: number }[] };

export interface TableInfo {
  name: string;
  schema: string;
  /** Defaults to table for catalog sources that do not report a relation kind. */
  relationType?: "table" | "view";
  columns: ColumnInfo[];
  rowCount: number;
  createdAt: string;
  columnStats?: ColumnStats[];
}

export interface DatabaseInfo {
  name: string;
  tables: TableInfo[];
}

//
// Query Types
//

export interface QueryResult {
  columns: string[];
  columnTypes: string[];
  data: Record<string, unknown>[];
  rowCount: number;
  error?: string;
  /**
   * The engine stopped early at a row cap — `data` is a prefix of the real
   * result, not the whole thing. Consumers that export or aggregate MUST say
   * so rather than presenting a partial answer as complete.
   */
  truncated?: boolean;
  /** Engine execution time. Absent for results restored from persistence. */
  durationMs?: number;
}

export interface QueryHistoryItem {
  id: string;
  query: string;
  timestamp: Date;
  error?: string;
}

export interface ExternalQueryResponse {
  meta: Array<{ name: string; type: string }>;
  data: unknown[][];
  rows?: number;
}

//
// File System Types
//

export interface MountedFolderInfo {
  id: string;
  name: string;
  addedAt: Date;
  hasPermission: boolean;
}

//
// Editor & Chart Types
//

export type EditorTabType =
  "sql" | "notebook" | "home" | "connections" | "settings" | "review" | "explorer";

export interface NotebookCell {
  id: string;
  type: "sql" | "markdown";
  content: string;
  result?: QueryResult | null;
  chartConfig?: ChartConfig;
  collapsed?: boolean;
}

export type ChartType =
  | "bar"
  | "line"
  | "pie"
  | "area"
  | "scatter"
  | "combo"
  | "stacked_bar"
  | "grouped_bar"
  | "stacked_area"
  | "donut"
  | "heatmap"
  | "treemap"
  | "funnel"
  | "gauge"
  | "box"
  | "bubble";

export type AggregationType = "sum" | "avg" | "count" | "min" | "max" | "none";
export type SortOrder = "asc" | "desc" | "none";
export type AxisScale = "linear" | "log";

export interface SeriesConfig {
  column: string;
  label?: string;
  color?: string;
  type?: "bar" | "line" | "area";
  yAxisId?: "left" | "right";
  aggregation?: AggregationType;
}

export interface AxisConfig {
  label?: string;
  scale?: AxisScale;
  min?: number;
  max?: number;
  format?: string;
  showGrid?: boolean;
  rotate?: number;
}

export interface LegendConfig {
  show?: boolean;
  position?: "top" | "bottom" | "left" | "right";
  align?: "start" | "center" | "end";
}

export interface AnnotationConfig {
  id: string;
  type: "line" | "text" | "box";
  value?: number;
  text?: string;
  x?: number;
  y?: number;
  color?: string;
}

export interface DataTransform {
  groupBy?: string;
  aggregation?: AggregationType;
  sortBy?: string;
  sortOrder?: SortOrder;
  limit?: number;
  filter?: string;
}

export interface ChartConfig {
  type: ChartType;
  xAxis: string;
  xAxisConfig?: AxisConfig;
  yAxis?: string;
  yAxisConfig?: AxisConfig;
  series?: SeriesConfig[];
  colorBy?: string;
  sizeBy?: string;
  transform?: DataTransform;
  colors?: string[];
  legend?: LegendConfig;
  showValues?: boolean;
  showGrid?: boolean;
  enableAnimations?: boolean;
  annotations?: AnnotationConfig[];
  stacked?: boolean;
  smooth?: boolean;
  innerRadius?: number;
  title?: string;
  subtitle?: string;
}

export interface EditorTab {
  id: string;
  title: string;
  type: EditorTabType;
  content: string | { database?: string; table?: string };
  result?: QueryResult | null;
  chartConfig?: ChartConfig;
}

//
// Slice Interfaces
//

export interface DuckdbSlice {
  /**
   * DuckDB-WASM handles for the active session, when it runs in this tab.
   * Compatibility projection only — SQL execution and introspection go through
   * `currentSession`. Null on connections that execute elsewhere.
   *
   * Per-connection engine lifecycle is owned by `services/engine/registry`,
   * which is why there are no longer separate wasm/opfs handle pairs here.
   */
  db: duckdb.AsyncDuckDB | null;
  connection: duckdb.AsyncDuckDBConnection | null;
  isInitialized: boolean;
  isLoading: boolean;
  error: string | null;
  currentDatabase: string;

  initialize: () => Promise<void>;
  cleanup: () => Promise<void>;
}

export interface ConnectionSlice {
  currentConnection: CurrentConnection | null;
  /**
   * Live engine session for `currentConnection`. Everything that executes SQL
   * or reads the catalog goes through this — never through `db`/`connection`,
   * which remain only as a compatibility projection for the DuckDB-WASM
   * specific code paths (file import, parquet export).
   */
  currentSession: DataSession | null;
  connectionList: ConnectionList;
  isLoadingExternalConnection: boolean;

  addConnection: (connection: ConnectionProvider) => Promise<void>;
  updateConnection: (connection: ConnectionProvider) => void;
  deleteConnection: (id: string) => void;
  setCurrentConnection: (connectionId: string) => Promise<void>;
  getConnection: (connectionId: string) => ConnectionProvider | undefined;
}

/** Live view of a query that is still running. */
export interface QueryProgress {
  rows: number;
  batches: number;
  elapsedMs: number;
  /** Column headers, known from the engine before the first row arrives. */
  columns: string[];
}

export interface QuerySlice {
  queryHistory: QueryHistoryItem[];
  executingTabs: Record<string, boolean>;
  /** Per-tab progress for in-flight queries. Cleared when one settles. */
  queryProgress: Record<string, QueryProgress>;
  /**
   * Rows an editor query may return before the engine stops and marks the
   * result truncated. Guards the tab against a `SELECT *` on a huge table
   * materializing into JS objects and freezing it.
   */
  maxResultRows: number;
  setMaxResultRows: (rows: number) => void;

  executeQuery: (query: string, tabId?: string) => Promise<QueryResult | void>;
  /** Cancels the in-flight query started for this tab, if any. */
  cancelQuery: (tabId: string) => Promise<void>;
  clearHistory: () => void;
  exportParquet: (query: string) => Promise<Blob>;
}

export interface SchemaSlice {
  databases: DatabaseInfo[];
  isLoadingDbTablesFetch: boolean;
  schemaFetchError: string | null;

  fetchDatabasesAndTablesInfo: () => Promise<void>;
  fetchTableColumnStats: (
    databaseName: string,
    tableName: string,
    schema?: string
  ) => Promise<ColumnStats[]>;
  fetchColumnDistribution: (
    databaseName: string,
    tableName: string,
    columnName: string,
    columnType: string,
    schema?: string
  ) => Promise<ColumnDistribution | null>;
  deleteTable: (tableName: string, database?: string, schema?: string) => Promise<void>;
  importFile: (
    fileName: string,
    fileContent: ArrayBuffer,
    tableName: string,
    fileType: string,
    database?: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    options?: Record<string, any>
  ) => Promise<void>;
}

export interface TabSlice {
  tabs: EditorTab[];
  activeTabId: string | null;

  createTab: (type?: EditorTabType, content?: string, title?: string) => string;
  closeTab: (tabId: string) => void;
  setActiveTab: (tabId: string) => void;
  updateTabQuery: (tabId: string, query: string) => void;
  updateTabTitle: (tabId: string, title: string) => void;
  updateTabChartConfig: (tabId: string, chartConfig: ChartConfig | undefined) => void;
  moveTab: (oldIndex: number, newIndex: number) => void;
  closeAllTabs: () => void;

  // Notebook cell operations
  getNotebookCells: (tabId: string) => NotebookCell[];
  addNotebookCell: (tabId: string, afterCellId?: string, cellType?: "sql" | "markdown") => void;
  removeNotebookCell: (tabId: string, cellId: string) => void;
  updateNotebookCellContent: (tabId: string, cellId: string, content: string) => void;
  updateNotebookCellResult: (tabId: string, cellId: string, result: QueryResult | null) => void;
  updateNotebookCellChartConfig: (
    tabId: string,
    cellId: string,
    chartConfig: ChartConfig | undefined
  ) => void;
  moveNotebookCell: (tabId: string, cellId: string, direction: "up" | "down") => void;
  toggleNotebookCellCollapsed: (tabId: string, cellId: string) => void;
  toggleNotebookCellType: (tabId: string, cellId: string) => void;
}

export interface FileSystemSlice {
  mountedFolders: MountedFolderInfo[];
  isFileSystemSupported: boolean;

  initFileSystem: () => Promise<void>;
  mountFolder: () => Promise<MountedFolderInfo | null>;
  unmountFolder: (id: string) => Promise<void>;
  refreshFolderPermissions: () => Promise<void>;
}

//
// Profile Types
//

export interface Profile {
  id: string;
  name: string;
  avatarEmoji: string;
  hasPassword: boolean;
  createdAt: string;
  lastActive: string;
}

export interface ProfileSlice {
  currentProfileId: string | null;
  currentProfile: Profile | null;
  profiles: Profile[];
  isProfileLoaded: boolean;
  encryptionKey: CryptoKey | null;

  loadProfile: (profileId: string, password?: string) => Promise<void>;
  createProfile: (name: string, password?: string, avatarEmoji?: string) => Promise<string>;
  deleteProfile: (profileId: string) => Promise<void>;
  switchProfile: (profileId: string, password?: string) => Promise<void>;
  updateProfile: (updates: Partial<Pick<Profile, "name" | "avatarEmoji">>) => Promise<void>;

  savedQueriesVersion: number;
  bumpSavedQueriesVersion: () => void;
}

export interface LakehouseSlice {
  portalAuth: PortalAuthState;
  lakehouseCatalog: LakehouseLayer[];
  /** Pinned immutable release, or legacy when no pointer exists. */
  lakehouseRelease: ReleaseCatalogResolution | null;
  /** Independently pinned source Landing snapshots and source-specific metadata errors. */
  lakehouseLanding: LandingCatalogResolution | null;
  /** Physical native files, independent of published DuckDB datasets. */
  nativeLandingRoot: NativeLandingFolder | null;
  nativeLandingFolders: Record<string, NativeLandingFolder>;
  nativeLandingChildren: Record<
    string,
    { files: NativeLandingFile[]; loaded: boolean; loading: boolean; error: string | null }
  >;
  nativeLandingLoading: boolean;
  nativeLandingError: string | null;
  nativeLandingSelected: string | null;
  nativeLandingActionError: string | null;
  nativeMetadataTables: Record<string, string>;
  nativeMetadataLoading: string | null;
  nativeMetadataProgress: NativeMetadataProgress | null;
  nativeMetadataError: string | null;
  isLakehouseLoading: boolean;
  lakehouseStatusMessage: string;
  activeLakehouseDataset: string | null;
  activeLakehouseLayer: string | null;

  /** Re-checks the Cloudflare Access session (after an expired or refused API call). */
  signIn: () => Promise<boolean>;
  refreshLakehouseCatalog: () => Promise<void>;
  refreshNativeLanding: () => Promise<void>;
  loadNativeLandingFolder: (folderId: string) => Promise<void>;
  previewNativeLandingFile: (folderId: string, fileId: string) => Promise<string | null>;
  loadNativeMetadataTable: (folderId: string) => Promise<string | null>;
  cancelNativeMetadataScan: () => void;
  toggleLakehouseLayer: (layerName: string) => Promise<void>;
  toggleLakehouseTable: (layerName: string, tableName: string) => Promise<void>;
  selectLakehouseDataset: (layerName: string, tableName: string) => Promise<string | null>;
  selectLakehouseFile: (
    layerName: string,
    tableName: string,
    fileId: string
  ) => Promise<string | null>;
  /** Loads only the named tables from the pinned release and opens an unexecuted SQL tab. */
  prepareLakehouseQuery: (
    tables: ReadonlyArray<{ layerName: string; tableName: string }>,
    sql: string,
    title: string
  ) => Promise<string | null>;
  /** Loads only pinned-release tables referenced by SQL, before the normal engine run. */
  preparePublishedTablesForQuery: (sql: string) => Promise<void>;
}

//
// Composed Store Type
//

export type DuckStoreState = DuckdbSlice &
  ConnectionSlice &
  QuerySlice &
  SchemaSlice &
  TabSlice &
  FileSystemSlice &
  ProfileSlice &
  LakehouseSlice;
