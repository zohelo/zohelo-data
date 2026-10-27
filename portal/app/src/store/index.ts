import { create, type StateCreator } from "zustand";
import { devtools } from "zustand/middleware";
import { createDuckdbSlice } from "./slices/duckdbSlice";
import { createConnectionSlice } from "./slices/connectionSlice";
import { createQuerySlice } from "./slices/querySlice";
import { createSchemaSlice } from "./slices/schemaSlice";
import { createTabSlice } from "./slices/tabSlice";
import { createFileSystemSlice } from "./slices/fileSystemSlice";
import { createProfileSlice } from "./slices/profileSlice";
import { createSessionSlice } from "./slices/sessionSlice";
import { createDashboardSlice } from "./slices/dashboardSlice";
import { createLakehouseSlice } from "./slices/lakehouseSlice";
import { saveWorkspace } from "@/services/persistence/repositories/workspaceRepository";
import { isSystemDbInitialized } from "@/services/persistence/systemDb";
import type { DuckStoreState } from "./types";

// Re-export all types from the centralized types file
export * from "./types";

const storeCreator: StateCreator<DuckStoreState, [["zustand/devtools", never]]> = (...a) => ({
  ...createDuckdbSlice(...a),
  ...createConnectionSlice(...a),
  ...createQuerySlice(...a),
  ...createSchemaSlice(...a),
  ...createTabSlice(...a),
  ...createFileSystemSlice(...a),
  ...createProfileSlice(...a),
  ...createSessionSlice(...a),
  ...createDashboardSlice(...a),
  ...createLakehouseSlice(...a),
});

export const useDuckStore = create<DuckStoreState>()(
  devtools(storeCreator, { enabled: import.meta.env.DEV })
);

// ─── Auto-save: debounced writes to system DB ────────────────────────────────

let saveTimer: ReturnType<typeof setTimeout>;
let lastSavedTabs: string | undefined;
async function persistWorkspaceState(state: DuckStoreState): Promise<void> {
  const { currentProfileId, isProfileLoaded } = state;
  if (!currentProfileId || !isProfileLoaded || !isSystemDbInitialized()) return;

  try {
    // Save workspace (tabs, active tab, current DB)
    const tabsJson = JSON.stringify(
      state.tabs.map((tab) => {
        if (tab.type === "notebook" && typeof tab.content === "string") {
          try {
            const cells = JSON.parse(tab.content) as Array<Record<string, unknown>>;
            const cleanCells = cells.map((c) => ({ ...c, result: undefined }));
            return { ...tab, result: undefined, content: JSON.stringify(cleanCells) };
          } catch {
            return { ...tab, result: undefined };
          }
        }
        return { ...tab, result: undefined };
      })
    );
    if (tabsJson !== lastSavedTabs) {
      await saveWorkspace(currentProfileId, {
        tabs: tabsJson,
        activeTabId: state.activeTabId,
        currentConnectionId: state.currentConnection?.id ?? null,
        currentDatabase: state.currentDatabase ?? null,
      });
      lastSavedTabs = tabsJson;
    }

  } catch (error) {
    console.warn("[AutoSave] Failed to persist state:", error);
  }
}

let autoSaveUnsubscribe: (() => void) | null = null;

export function startAutoSave(): void {
  // Prevent duplicate subscriptions
  if (autoSaveUnsubscribe) return;
  autoSaveUnsubscribe = useDuckStore.subscribe((state) => {
    if (!state.isProfileLoaded) return;
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => persistWorkspaceState(state), 2000);
  });
}

// Also save on page unload
if (typeof window !== "undefined") {
  window.addEventListener("beforeunload", () => {
    const state = useDuckStore.getState();
    if (state.isProfileLoaded) {
      // Best-effort sync save (may be truncated by browser)
      persistWorkspaceState(state);
    }
  });
}
