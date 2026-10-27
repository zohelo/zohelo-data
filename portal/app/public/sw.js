// Retires the service worker that earlier portal builds installed (vite-plugin-pwa).
// A browser that still runs it checks this URL for updates, installs this worker, which
// deletes the old caches, unregisters itself and reloads open tabs from the network.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      const names = await caches.keys();
      await Promise.all(
        names
          .filter((name) => /^(workbox-|duckui-|duckdb-extensions$|jsdelivr-cdn$|webllm)/.test(name))
          .map((name) => caches.delete(name))
      );
      await self.registration.unregister();
      const tabs = await self.clients.matchAll({ type: "window" });
      await Promise.all(tabs.map((tab) => tab.navigate(tab.url)));
    })()
  );
});
