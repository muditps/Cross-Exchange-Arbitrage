import { memo } from 'react';
import type { LivePriceMap } from '@/hooks/useLivePrices';
import type { ExchangeId } from '@/types';

interface CrossExchangeSpreadMatrixProps {
  /** Live bid/ask prices per exchange for the currently selected pair. */
  prices: LivePriceMap;
  /** The trading pair being displayed (e.g. "BTC-USDT"). */
  tradingPair: string;
}

const EXCHANGES: ExchangeId[] = ['BINANCE', 'BYBIT', 'KUCOIN'];

/** Taker fee rate used by all three exchanges in the current config (0.10%). */
const TAKER_FEE_RATE = 0.001;

/** Price older than this threshold is considered stale for display purposes. */
const STALE_THRESHOLD_MS = 1_000;

interface SpreadResult {
  netSpreadBps: number;
  isStale: boolean;
  isMissing: boolean;
}

function computeNetSpreadBps(
  prices: LivePriceMap,
  buyExchange: ExchangeId,
  sellExchange: ExchangeId
): SpreadResult {
  const buyEntry = prices[buyExchange];
  const sellEntry = prices[sellExchange];

  if (!buyEntry || !sellEntry) return { netSpreadBps: 0, isStale: false, isMissing: true };

  const now = Date.now();
  const isStale = now - buyEntry.receivedAt > STALE_THRESHOLD_MS ||
                  now - sellEntry.receivedAt > STALE_THRESHOLD_MS;

  const buyAsk  = parseFloat(buyEntry.tick.bestAskPrice);
  const sellBid = parseFloat(sellEntry.tick.bestBidPrice);
  if (isNaN(buyAsk) || isNaN(sellBid) || buyAsk <= 0) {
    return { netSpreadBps: 0, isStale, isMissing: true };
  }

  const grossSpread = sellBid - buyAsk;
  const totalFee    = buyAsk * TAKER_FEE_RATE + sellBid * TAKER_FEE_RATE;
  const netSpreadBps = ((grossSpread - totalFee) / buyAsk) * 10_000;

  return { netSpreadBps, isStale, isMissing: false };
}

/**
 * Displays a cross-exchange net spread matrix for the selected trading pair.
 *
 * Rows are the BUY exchange, columns are the SELL exchange. Each cell shows the net
 * spread in basis points after subtracting 0.1% taker fees on both legs — matching the
 * fee model used by the detection engine. Green cells indicate a net-positive opportunity;
 * red cells are below-fee-threshold.
 *
 * The computation is purely client-side from live prices — no additional backend call is
 * needed. Stale prices (>1s) are dimmed so users can see which legs have data quality issues.
 *
 * @param prices    Current per-exchange bid/ask prices for the selected pair
 * @param tradingPair   The trading pair being monitored (e.g. "BTC-USDT")
 */
export const CrossExchangeSpreadMatrix = memo(function CrossExchangeSpreadMatrix({ prices, tradingPair }: CrossExchangeSpreadMatrixProps) {
  return (
    <div>
      <h2 className="text-base font-semibold text-gray-300 mb-3">
        Cross-Exchange Spread Matrix
        <span className="ml-2 text-xs font-normal text-gray-500">
          {tradingPair} · Net spread after 0.1% taker fees
        </span>
      </h2>

      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr>
              <th className="px-3 py-2 text-left text-gray-500 font-normal w-28">
                Buy ↓ / Sell →
              </th>
              {EXCHANGES.map((sellEx) => (
                <th key={sellEx} className="px-3 py-2 text-center text-gray-400 font-semibold tracking-wide uppercase">
                  {sellEx}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {EXCHANGES.map((buyEx) => (
              <tr key={buyEx} className="border-t border-surface-border/40">
                <td className="px-3 py-2 text-gray-400 font-semibold uppercase tracking-wide">
                  {buyEx}
                </td>
                {EXCHANGES.map((sellEx) => {
                  if (buyEx === sellEx) {
                    return (
                      <td key={sellEx} className="px-3 py-2.5 text-center text-gray-700">
                        —
                      </td>
                    );
                  }

                  const { netSpreadBps, isStale, isMissing } = computeNetSpreadBps(prices, buyEx, sellEx);

                  if (isMissing) {
                    return (
                      <td key={sellEx} className="px-3 py-2.5 text-center text-gray-600">
                        —
                      </td>
                    );
                  }

                  const isPositive = netSpreadBps > 0;
                  const cellClass = isStale
                    ? 'text-gray-600'
                    : isPositive
                    ? 'text-accent-green font-semibold'
                    : 'text-accent-red';

                  return (
                    <td key={sellEx} className={`px-3 py-2.5 text-center font-mono ${cellClass}`}>
                      <span title={isStale ? 'Price data is stale (>1s)' : undefined}>
                        {isPositive ? '+' : ''}{netSpreadBps.toFixed(2)}
                        <span className="ml-0.5 text-gray-600 font-normal">bps</span>
                      </span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p className="mt-2 text-xs text-gray-600">
        Positive = net-profitable after fees. Stale prices (&gt;1s) shown dimmed. Source: live /ws/ticks feed.
      </p>
    </div>
  );
});
