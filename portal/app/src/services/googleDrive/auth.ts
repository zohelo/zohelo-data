/**
 * Portal sign-in. Cloudflare Access signs the user in before the portal loads; every request to
 * the portal's /api carries the Access session cookie, and the API checks the signed-in email.
 * The app keeps its token-based flow, with a fixed token that only means "signed in through Access".
 */

export const DRIVE_ROOT = "zohelo-data";
export const LAKEHOUSE_LAYERS = [
  "01_landing",
  "02_bronze",
  "03_silver",
  "04_gold",
  "05_archive",
] as const;

export const ACCESS_SESSION_TOKEN = "cloudflare-access";

let signedOut = false;

export const getStoredToken = (): string | null => (signedOut ? null : ACCESS_SESSION_TOKEN);

export const isStoredTokenExpired = (_token: string): boolean => false;

export const setStoredToken = (_token: string | null, _expiresAt?: number): void => {
  signedOut = false;
};

export const clearStoredToken = (): void => {
  signedOut = true;
};

export const clearStoredTokenIfCurrent = (token: string): boolean => {
  if (token !== ACCESS_SESSION_TOKEN) return false;
  clearStoredToken();
  return true;
};

/** "Sign in": the Access session already exists; a reload renews it if it has expired. */
export const requestGoogleAccessToken = async (_options?: { promptConsent?: boolean }): Promise<string> => {
  if (signedOut) {
    signedOut = false;
    const probe = await fetch("/api/health", { redirect: "manual" }).catch(() => null);
    if (!probe || !probe.ok) {
      window.location.reload();
      throw new Error("Renewing the sign-in…");
    }
  }
  return ACCESS_SESSION_TOKEN;
};
