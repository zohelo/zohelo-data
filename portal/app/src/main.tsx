import "./index.css";
import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { Suspense } from "react";
import { ErrorBoundary, FallbackProps } from "react-error-boundary";
import { ThemeProvider } from "@/components/theme/theme-provider";
import { Routes, Route } from "react-router";
import { useDuckStore, startAutoSave } from "./store";
import Home from "@/pages/Home";
import { Toaster } from "@/components/ui/sonner";
import { Loader2, AlertTriangle, RefreshCw } from "lucide-react";
import { Navigate } from "react-router";
import { initializeSystemDb } from "@/services/persistence/systemDb";
import { cleanupOrphanedStorage } from "@/services/persistence/cleanup";
import { listProfiles } from "@/services/persistence/repositories/profileRepository";
import ProfilePicker from "@/components/profile/ProfilePicker";
import type { Profile } from "@/store/types";

interface LoadingScreenProps {
  message: string;
}

interface AppInitializerProps {
  children: React.ReactNode;
}

const LoadingScreen = ({ message }: LoadingScreenProps) => (
  <div className="h-screen flex items-center justify-center bg-background text-foreground">
    <div className="text-center">
      <Loader2 className="animate-spin m-auto mb-12" size={64} />
      <p className="text-lg">{message}</p>
    </div>
  </div>
);

const ErrorFallback = ({ error, resetErrorBoundary }: FallbackProps) => (
  <div className="h-screen flex items-center justify-center bg-background text-foreground">
    <div className="text-center max-w-md p-6">
      <AlertTriangle className="mx-auto mb-4 text-destructive" size={48} />
      <h2 className="text-xl font-semibold mb-2">Something went wrong</h2>
      <p className="text-muted-foreground mb-4 text-sm">
        {error instanceof Error
          ? error.message
          : "An unexpected error occurred while rendering the application."}
      </p>
      <button
        onClick={resetErrorBoundary}
        className="inline-flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
      >
        <RefreshCw size={16} />
        Try again
      </button>
    </div>
  </div>
);

/**
 * ProfileBootstrap — initializes the system database and loads the user's profile
 * before the main app starts. Handles first-time setup and localStorage migration.
 * When multiple profiles exist, shows a profile picker screen.
 */
const ProfileBootstrap = ({ children }: { children: React.ReactNode }) => {
  const [ready, setReady] = useState(false);
  const [showPicker, setShowPicker] = useState(false);
  const [bootProfiles, setBootProfiles] = useState<Profile[]>([]);
  const createProfile = useDuckStore((s) => s.createProfile);
  const loadProfile = useDuckStore((s) => s.loadProfile);
  const bootedRef = useRef(false);

  useEffect(() => {
    if (bootedRef.current) return;
    bootedRef.current = true;

    async function boot() {
      try {
        await initializeSystemDb();
        // Clears storage left by removed features (see cleanup.ts). Never
        // blocks boot.
        cleanupOrphanedStorage().catch(() => {});
        const profiles = await listProfiles();

        if (profiles.length === 1 && !profiles[0].has_password) {
          // Single unprotected profile: auto-load
          await loadProfile(profiles[0].id);
          startAutoSave();
          setReady(true);
        } else {
          // 0 profiles, multiple profiles, or single password-protected: show picker
          setBootProfiles(
            profiles.map((p) => ({
              id: p.id,
              name: p.name,
              avatarEmoji: p.avatar_emoji,
              hasPassword: p.has_password,
              createdAt: p.created_at,
              lastActive: p.last_active,
            }))
          );
          setShowPicker(true);
        }
      } catch (error) {
        console.error("[ProfileBootstrap] Failed to initialize profile:", error);
        setReady(true);
      }
    }

    boot();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  if (!ready && !showPicker) {
    return <LoadingScreen message="Loading profile..." />;
  }

  if (showPicker && !ready) {
    return (
      <ThemeProvider defaultTheme="system" storageKey="vite-ui-theme">
        <ProfilePicker
          profiles={bootProfiles}
          onSelectProfile={async (id, password) => {
            await loadProfile(id, password);
            startAutoSave();
            setReady(true);
            setShowPicker(false);
          }}
          onCreateProfile={async (name, password, avatarEmoji) => {
            const id = await createProfile(name, password, avatarEmoji);
            await loadProfile(id, password);
            startAutoSave();
            setReady(true);
            setShowPicker(false);
            return id;
          }}
        />
      </ThemeProvider>
    );
  }

  return children;
};

const AppInitializer = ({ children }: AppInitializerProps) => {
  const initialize = useDuckStore((s) => s.initialize);
  const isInitialized = useDuckStore((s) => s.isInitialized);
  const isLoading = useDuckStore((s) => s.isLoading);

  useEffect(() => {
    initialize();
  }, [initialize]);

  if (isLoading || !isInitialized) {
    return <LoadingScreen message="Initializing DuckDB" />;
  }

  return children;
};

const App = () => {
  useEffect(() => {
    const handleBeforeUnload = (e: BeforeUnloadEvent) => {
      // Only prompt when there are SQL tabs with unsaved content
      const state = useDuckStore.getState();
      const hasUnsavedWork = state.tabs.some(
        (t) => t.type === "sql" && typeof t.content === "string" && t.content.trim().length > 0
      );
      if (hasUnsavedWork) {
        e.preventDefault();
      }
    };

    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => window.removeEventListener("beforeunload", handleBeforeUnload);
  }, []);

  return (
    <div className="flex flex-col w-full h-screen overflow-hidden">
      <Toaster richColors toastOptions={{ duration: 2000, closeButton: true }} expand={true} />
      <div className="flex-1 overflow-hidden">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </div>
  );
};

// The portal used to install a service worker (PWA). It is gone: unregister any worker a
// previous visit installed and drop its caches, so old builds and sign-in state never stick.
// public/sw.js does the same for browsers still running the old worker.
if ("serviceWorker" in navigator) {
  void navigator.serviceWorker
    .getRegistrations()
    .then((registrations) => Promise.all(registrations.map((r) => r.unregister())))
    .catch(() => undefined);
}
const isRetiredCache = (name: string) =>
  /^(workbox-|duckui-|duckdb-extensions$|jsdelivr-cdn$|webllm)/.test(name);
if ("caches" in window) {
  void caches
    .keys()
    .then((keys) => Promise.all(keys.filter(isRetiredCache).map((key) => caches.delete(key))))
    .catch(() => undefined);
}

const rootElement = document.getElementById("root");
if (!rootElement) throw new Error("Failed to find root element");

// Vite's relative asset base is not a router path: React Router interprets
// "./" as "/./" and renders a blank page. Explicit subpath bases still apply.
const routerBasename = import.meta.env.BASE_URL === "./" ? "/" : import.meta.env.BASE_URL;

const FullApp = () => (
  <StrictMode>
    <ErrorBoundary FallbackComponent={ErrorFallback}>
      <ProfileBootstrap>
        <AppInitializer>
          <BrowserRouter basename={routerBasename}>
            <ThemeProvider defaultTheme="system" storageKey="vite-ui-theme">
              <Suspense fallback={<LoadingScreen message="Loading application" />}>
                <App />
              </Suspense>
            </ThemeProvider>
          </BrowserRouter>
        </AppInitializer>
      </ProfileBootstrap>
    </ErrorBoundary>
  </StrictMode>
);

// Production render
createRoot(rootElement).render(<FullApp />);
