import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import './index.css';
import App from './App.tsx';

/**
 * TanStack Query client configuration.
 *
 * staleTime 30s: REST data (historical opportunities, analytics) is considered
 * fresh for 30 seconds before a background re-fetch. Real-time data comes via
 * WebSocket — those hooks do NOT use TanStack Query fetch, so staleTime is
 * irrelevant for the live feed.
 *
 * retry 1: failed API calls retry once before surfacing an error state.
 * The dashboard is read-only — a single retry is sufficient; aggressive retries
 * just delay showing the error to the user.
 */
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 1,
    },
  },
});

const rootElement = document.getElementById('root');
if (!rootElement) throw new Error('Root element #root not found in index.html');

createRoot(rootElement).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
