import { useCallback, useSyncExternalStore } from "react";
import { parseReviewDraft, type BusinessReviewDraft } from "./businessReview";

interface BusinessReviewSnapshot {
  draft: BusinessReviewDraft;
  saved: boolean | null;
}

interface ReviewStore {
  snapshot: BusinessReviewSnapshot;
  listeners: Set<() => void>;
}

const reviewStores = new Map<string, ReviewStore>();

function readReviewStore(storageKey: string): ReviewStore {
  const existing = reviewStores.get(storageKey);
  if (existing) return existing;

  let snapshot: BusinessReviewSnapshot;
  if (typeof localStorage === "undefined") {
    snapshot = { draft: parseReviewDraft(null), saved: false };
  } else {
    try {
      const raw = localStorage.getItem(storageKey);
      snapshot = { draft: parseReviewDraft(raw), saved: raw ? true : null };
    } catch {
      snapshot = { draft: parseReviewDraft(null), saved: false };
    }
  }

  const store = { snapshot, listeners: new Set<() => void>() };
  reviewStores.set(storageKey, store);
  return store;
}

function getBusinessReviewSnapshot(storageKey: string): BusinessReviewSnapshot {
  return readReviewStore(storageKey).snapshot;
}

function subscribeToBusinessReview(storageKey: string, listener: () => void): () => void {
  const store = readReviewStore(storageKey);
  store.listeners.add(listener);
  return () => store.listeners.delete(listener);
}

/** All responsive workspace mounts for one profile observe the same local draft. */
export function useBusinessReviewDraft(storageKey: string): BusinessReviewSnapshot {
  const subscribe = useCallback(
    (listener: () => void) => subscribeToBusinessReview(storageKey, listener),
    [storageKey]
  );
  const getSnapshot = useCallback(() => getBusinessReviewSnapshot(storageKey), [storageKey]);
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
