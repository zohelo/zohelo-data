import { StateCreator } from "zustand";
import type { DuckStoreState, ProfileSlice, Profile } from "../types";
import {
  createProfile as createProfileRepo,
  getProfile as getProfileRepo,
  listProfiles as listProfilesRepo,
  updateProfile as updateProfileRepo,
  deleteProfile as deleteProfileRepo,
  updateLastActive,
} from "@/services/persistence/repositories/profileRepository";
import { getConnections } from "@/services/persistence/repositories/connectionRepository";
import { getHistory } from "@/services/persistence/repositories/queryHistoryRepository";
import { loadWorkspace } from "@/services/persistence/repositories/workspaceRepository";
import { getSettingsByCategory } from "@/services/persistence/repositories/settingsRepository";
import {
  generateEncryptionKey,
  deriveKeyFromPassword,
  generateSalt,
  encrypt,
  decrypt,
  storeKeyForProfile,
  loadKeyForProfile,
  deleteKeyForProfile,
} from "@/services/persistence/crypto";
import type {
  EditorTab,
  ConnectionProvider,
  QueryHistoryItem,
} from "../types";

const VERIFY_TOKEN_PLAINTEXT = "duck-ui-profile-verify";

export const createProfileSlice: StateCreator<
  DuckStoreState,
  [["zustand/devtools", never]],
  [],
  ProfileSlice
> = (set, get) => ({
  currentProfileId: null,
  currentProfile: null,
  profiles: [],
  isProfileLoaded: false,
  encryptionKey: null,
  savedQueriesVersion: 0,

  bumpSavedQueriesVersion: () => set({ savedQueriesVersion: get().savedQueriesVersion + 1 }),

  createProfile: async (name, password, avatarEmoji) => {
    let cryptoKey: CryptoKey;
    let salt: Uint8Array | undefined;
    let verifyToken: string | null = null;

    if (password) {
      salt = generateSalt();
      cryptoKey = await deriveKeyFromPassword(password, salt);
      verifyToken = await encrypt(VERIFY_TOKEN_PLAINTEXT, cryptoKey);
    } else {
      cryptoKey = await generateEncryptionKey();
    }

    const profile = await createProfileRepo(name, avatarEmoji || "logo", !!password, verifyToken);
    await storeKeyForProfile(profile.id, cryptoKey, salt);

    const profiles = await listProfilesRepo();
    set({
      profiles: profiles.map(mapProfile),
    });

    return profile.id;
  },

  loadProfile: async (profileId, password) => {
    const profile = await getProfileRepo(profileId);
    if (!profile) throw new Error("Profile not found");

    // Load or derive encryption key
    let cryptoKey: CryptoKey;
    const stored = await loadKeyForProfile(profileId);

    if (profile.has_password && password) {
      if (!stored?.salt) throw new Error("No salt found for password-protected profile");
      cryptoKey = await deriveKeyFromPassword(password, stored.salt);
      // Verify password by decrypting the stored verify token
      if (profile.password_verify_token) {
        try {
          const decrypted = await decrypt(profile.password_verify_token, cryptoKey);
          if (decrypted !== VERIFY_TOKEN_PLAINTEXT) {
            throw new Error("Incorrect password");
          }
        } catch {
          throw new Error("Incorrect password");
        }
      }
    } else if (stored?.key) {
      cryptoKey = stored.key;
    } else {
      // No key stored, generate a new one
      cryptoKey = await generateEncryptionKey();
      await storeKeyForProfile(profileId, cryptoKey);
    }

    // Load workspace state
    const workspace = await loadWorkspace(profileId);
    if (workspace) {
      try {
        // Tab types that no longer exist (the old Brain settings tab) are
        // dropped rather than rendered as a mystery blank.
        const validTypes = new Set([
          "sql",
          "home",
          "connections",
          "settings",
          "review",
          "explorer",
        ]);
        const tabs = (JSON.parse(workspace.tabs) as EditorTab[])
          .filter((tab) => validTypes.has(tab.type))
          .map((tab) => ({
            ...tab,
            title:
              tab.type === "review" ? "Saved project input" : tab.title,
          }));
        set({
          tabs: tabs.length > 0 ? tabs : [{ id: "home", title: "Home", type: "home", content: "" }],
          activeTabId: workspace.active_tab_id ?? tabs[0]?.id ?? "home",
          currentDatabase: workspace.current_database ?? "memory",
        });
      } catch {
        // Invalid workspace JSON, use defaults
      }
    }

    // Load connections
    const savedConns = await getConnections(profileId, cryptoKey);
    if (savedConns.length > 0) {
      const connections: ConnectionProvider[] = savedConns.map((c) => ({
        environment: (c.environment as "APP" | "ENV" | "BUILT_IN") ?? "APP",
        id: c.id,
        name: c.name,
        scope: c.scope as "WASM" | "External" | "OPFS",
        ...(c.config as Record<string, unknown>),
        ...(c.credentials ?? {}),
      }));

      set({
        connectionList: { connections },
      });
    }

    // Load query history
    const history = await getHistory(profileId, 100);
    if (history.length > 0) {
      const queryHistory: QueryHistoryItem[] = history.map((h) => ({
        id: h.id,
        query: h.sql_text,
        timestamp: new Date(h.executed_at),
        ...(h.error ? { error: h.error } : {}),
      }));
      set({ queryHistory });
    }

    // Load theme setting
    const themeSettings = await getSettingsByCategory(profileId, "theme");
    if (themeSettings.mode) {
      try {
        const theme = JSON.parse(themeSettings.mode);
        localStorage.setItem("vite-ui-theme", theme);
      } catch {
        // Invalid theme value
      }
    }

    await updateLastActive(profileId);

    const allProfiles = await listProfilesRepo();

    set({
      currentProfileId: profileId,
      currentProfile: mapProfile(profile),
      profiles: allProfiles.map(mapProfile),
      isProfileLoaded: true,
      encryptionKey: cryptoKey,
    });
  },

  switchProfile: async (profileId, password) => {
    set({
      isProfileLoaded: false,
      currentProfileId: null,
      currentProfile: null,
      encryptionKey: null,
    });
    await get().loadProfile(profileId, password);
  },

  deleteProfile: async (profileId) => {
    await deleteProfileRepo(profileId);
    await deleteKeyForProfile(profileId);

    const profiles = await listProfilesRepo();
    set({ profiles: profiles.map(mapProfile) });
  },

  updateProfile: async (updates) => {
    const { currentProfileId } = get();
    if (!currentProfileId) return;

    await updateProfileRepo(currentProfileId, {
      ...(updates.name !== undefined ? { name: updates.name } : {}),
      ...(updates.avatarEmoji !== undefined ? { avatar_emoji: updates.avatarEmoji } : {}),
    });

    const profile = await getProfileRepo(currentProfileId);
    if (profile) {
      set({ currentProfile: mapProfile(profile) });
    }

    const profiles = await listProfilesRepo();
    set({ profiles: profiles.map(mapProfile) });
  },
});

function mapProfile(p: {
  id: string;
  name: string;
  avatar_emoji: string;
  has_password: boolean;
  created_at: string;
  last_active: string;
}): Profile {
  return {
    id: p.id,
    name: p.name,
    avatarEmoji: p.avatar_emoji,
    hasPassword: p.has_password,
    createdAt: p.created_at,
    lastActive: p.last_active,
  };
}
