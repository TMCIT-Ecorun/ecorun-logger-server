/// <reference lib="webworker" />

const worker = self as unknown as ServiceWorkerGlobalScope;
const CACHE = 'ecorun-shell-v1';
const SHELL = ['/', '/manifest.webmanifest', '/icon.svg'];

worker.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE).then((cache) => cache.addAll(SHELL)));
  worker.skipWaiting();
});

worker.addEventListener('activate', (event) => {
  event.waitUntil(caches.keys().then((keys) => Promise.all(keys.filter((key) => key !== CACHE).map((key) => caches.delete(key)))));
  worker.clients.claim();
});

worker.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  event.respondWith(fetch(event.request).catch(() => caches.match(event.request).then((cached) => cached ?? new Response('offline', { status: 503 }))));
});
