import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import path from 'path';

/**
 * Vite configuration.
 *
 * proxy: forwards /api/* and /ws/* to the Spring Boot backend on :8080.
 * This avoids CORS preflight in development even though WebFluxConfig also
 * allows localhost:5173. Using a proxy is cleaner — requests look same-origin
 * to the browser, and we avoid maintaining allowed-origin lists in two places.
 *
 * path alias: '@/' maps to 'src/' so imports read as '@/types' instead of
 * '../../types'. Consistent across every file depth.
 */
export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts', 'src/**/*.test.tsx'],
    alias: { '@': new URL('./src', import.meta.url).pathname },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:8080',
        ws: true,
        changeOrigin: true,
      },
      '/actuator': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
});
