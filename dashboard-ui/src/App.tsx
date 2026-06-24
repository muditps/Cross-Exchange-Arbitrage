import { BrowserRouter, NavLink, Navigate, Route, Routes } from 'react-router-dom';
import { LiveMonitorPage } from './pages/LiveMonitorPage';
import { OpportunitiesPage } from './pages/OpportunitiesPage';
import { AnalyticsPage } from './pages/AnalyticsPage';
import { HealthPage } from './pages/HealthPage';
import { useArbitrageStore } from './stores/arbitrageStore';
import { WebSocketProvider } from './context/WebSocketProvider';

const NAV_LINKS = [
  { to: '/monitor', label: 'Live Monitor' },
  { to: '/opportunities', label: 'Opportunities' },
  { to: '/analytics', label: 'Analytics' },
  { to: '/health', label: 'Health' },
] as const;

const WS_STATUS_COLOURS: Record<string, string> = {
  CONNECTED: 'bg-accent-green',
  CONNECTING: 'bg-accent-yellow',
  DISCONNECTED: 'bg-gray-500',
  ERROR: 'bg-accent-red',
};

/**
 * Root application shell: top nav + page routing.
 *
 * Layout: fixed top navbar with connection status indicator, full-height
 * content area below.
 */
export default function App() {
  const wsStatus = useArbitrageStore((s) => s.wsStatus);
  const dotColour = WS_STATUS_COLOURS[wsStatus] ?? 'bg-gray-500';

  return (
    <BrowserRouter>
      <WebSocketProvider>
      <div className="min-h-screen flex flex-col bg-surface text-gray-200">
        {/* ── Top navigation bar ── */}
        <header className="h-14 flex items-center px-6 border-b border-surface-border bg-surface-raised shrink-0">
          <span className="text-white font-semibold text-sm tracking-wide mr-8">
            Arbitrage Detector
          </span>

          <nav className="flex gap-1 flex-1">
            {NAV_LINKS.map(({ to, label }) => (
              <NavLink
                key={to}
                to={to}
                className={({ isActive }) =>
                  [
                    'px-3 py-1.5 rounded text-sm transition-colors',
                    isActive
                      ? 'bg-surface text-white'
                      : 'text-gray-400 hover:text-gray-200 hover:bg-surface',
                  ].join(' ')
                }
              >
                {label}
              </NavLink>
            ))}
          </nav>

          {/* WebSocket connection status indicator */}
          <div className="flex items-center gap-2 text-xs text-gray-400">
            <span className={`w-2 h-2 rounded-full ${dotColour}`} />
            {wsStatus}
          </div>
        </header>

        {/* ── Page content ── */}
        <main className="flex-1 overflow-auto">
          <Routes>
            <Route path="/" element={<Navigate to="/monitor" replace />} />
            <Route path="/monitor" element={<LiveMonitorPage />} />
            <Route path="/opportunities" element={<OpportunitiesPage />} />
            <Route path="/analytics" element={<AnalyticsPage />} />
            <Route path="/health" element={<HealthPage />} />
          </Routes>
        </main>
      </div>
      </WebSocketProvider>
    </BrowserRouter>
  );
}
