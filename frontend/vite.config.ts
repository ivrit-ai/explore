import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

// In dev, Vite serves the UI and forwards server routes to FastAPI (run.py --dev, port 5000).
const backend = 'http://127.0.0.1:5000';
const proxied = ['/api', '/audio', '/authorize', '/logout', '/xhost-auth'];

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: Object.fromEntries(proxied.map((path) => [path, { target: backend }])),
  },
  // Process stylesheets in tests so ?raw imports return their text (colour-token check).
  test: { css: true },
  build: {
    outDir: 'dist',
    // No inline module-preload polyfill: the CSP only allows same-origin scripts.
    modulePreload: { polyfill: false },
  },
});
