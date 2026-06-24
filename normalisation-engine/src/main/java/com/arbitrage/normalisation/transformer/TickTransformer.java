package com.arbitrage.normalisation.transformer;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;

import java.util.Optional;

/**
 * Contract for per-exchange normalisation transformation.
 *
 * <p>Each implementation encapsulates any exchange-specific post-processing
 * required after a tick arrives at the normalisation engine from Kafka. At minimum,
 * every transformer re-stamps the {@code processedTimestamp} to the current
 * {@code System.nanoTime()} — recording when the normalisation engine processed
 * the tick (T4 in the end-to-end latency chain).
 *
 * <p><b>Timestamp chain:</b>
 * <ul>
 *   <li>T0 ({@code receivedTimestamp}) — captured by the exchange connector when the
 *       WebSocket message arrives. Ground truth for staleness. MUST be preserved.</li>
 *   <li>T1 (original {@code processedTimestamp}) — when the connector finished parsing.</li>
 *   <li>T3 ({@code normalisationStartNanos}) — when the normalisation engine polled
 *       the tick from Kafka. Passed in by the caller.</li>
 *   <li>T4 (new {@code processedTimestamp}) — when this transformer finishes. Set by
 *       each implementation as {@code System.nanoTime()}.</li>
 * </ul>
 *
 * <p><b>Why per-exchange implementations?</b> Currently each transformer is a
 * pass-through with re-stamping. The interface exists as an extension point for
 * exchange-specific enrichment that may be needed in future phases:
 * <ul>
 *   <li>Configurable fee rate injection (overriding the default in {@link ExchangeId})</li>
 *   <li>Symbol canonicalisation for new asset classes (Phase 7: NSE, BSE)</li>
 *   <li>Per-exchange staleness threshold enforcement</li>
 *   <li>Clock skew correction using exchange-provided timestamps</li>
 * </ul>
 *
 * <p><b>Error contract:</b> Implementations return {@link Optional#empty()} for ticks
 * that cannot be processed (wrong exchange, null fields). They do NOT throw exceptions —
 * a single bad tick must not interrupt the normalisation pipeline.
 *
 * <p><b>Thread safety:</b> Implementations MUST be stateless and thread-safe.
 * The {@link TickTransformerFactory} shares one instance per exchange across all
 * Kafka listener threads.
 */
public interface TickTransformer {

    /**
     * Returns the {@link ExchangeId} this transformer handles.
     *
     * <p>Used by {@link TickTransformerFactory} to route inbound ticks to the
     * correct implementation. A transformer must only process ticks from its
     * declared exchange — {@link #transform} returns empty for any other exchange.</p>
     *
     * @return the exchange this transformer is responsible for
     */
    ExchangeId supports();

    /**
     * Transforms an inbound tick from the Kafka raw-ticks topic into a normalised
     * tick ready for the {@code normalised-ticks} topic.
     *
     * <p>Implementations must:</p>
     * <ol>
     *   <li>Return {@link Optional#empty()} if {@code inboundTick.getExchangeId() != supports()}.</li>
     *   <li>Preserve {@code receivedTimestamp} (T0) — it is the staleness anchor.</li>
     *   <li>Set {@code processedTimestamp} to {@code System.nanoTime()} (T4).</li>
     *   <li>Pass all price and quantity fields through unchanged.</li>
     * </ol>
     *
     * @param inboundTick           the tick consumed from the raw-ticks Kafka topic
     * @param normalisationStartNanos T3 — {@code System.nanoTime()} when the normalisation
     *                              engine polled this tick from Kafka (available for latency
     *                              metrics; not stored in the returned tick)
     * @return the transformed tick ready for {@code normalised-ticks}, or empty if the
     *         tick cannot be processed
     */
    Optional<NormalisedTick> transform(NormalisedTick inboundTick, long normalisationStartNanos);
}
