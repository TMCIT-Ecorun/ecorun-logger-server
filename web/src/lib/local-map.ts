import type * as Leaflet from 'leaflet';

let leaflet: typeof Leaflet | null = null;

/** Load Leaflet only in the browser so SvelteKit SSR never evaluates its DOM code. */
export async function loadLeaflet() {
  if (!leaflet) {
    const module = await import('leaflet') as unknown as { default?: typeof Leaflet } & typeof Leaflet;
    leaflet = module.default ?? module;
    await import('leaflet/dist/leaflet.css');
  }
  return leaflet;
}

/** Leaflet is browser-only; call loadLeaflet() from onMount before using L. */
export const L = new Proxy({} as typeof Leaflet, {
  get(_target, property: keyof typeof Leaflet) {
    if (!leaflet) throw new Error('Leaflet has not been loaded yet');
    return leaflet[property];
  }
});
