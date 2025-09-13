// docs/sw.js (v2) — network-first for HTML to avoid stale pages
const CACHE = 'asxshorts-v3';  // bump to invalidate old cache
const PRECACHE = [
  // Intentionally NOT caching './' or './index.html' to always fetch fresh HTML
  // List static assets (icons/css) here if you add them later.
];

self.addEventListener('install', (e) => {
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then(cache => cache.addAll(PRECACHE)).catch(()=>{}));
});

self.addEventListener('activate', (e) => {
  clients.claim();
  e.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
  );
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  const accept = req.headers.get('accept') || '';
  const isHTML = req.mode === 'navigate' || accept.includes('text/html') || req.destination === 'document';

  if (isHTML) {
    // Network-first for HTML documents (avoids serving stale index.html)
    e.respondWith(
      fetch(req).then(resp => resp).catch(() => caches.match(req))
    );
    return;
  }

  // For non-HTML: cache-first with network fallback, then cache the response
  e.respondWith(
    caches.match(req).then(cached => {
      if (cached) return cached;
      return fetch(req).then(resp => {
        const copy = resp.clone();
        caches.open(CACHE).then(c => c.put(req, copy));
        return resp;
      });
    }).catch(() => fetch(req))
  );
});
