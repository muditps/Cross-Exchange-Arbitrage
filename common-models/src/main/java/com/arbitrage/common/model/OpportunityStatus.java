package com.arbitrage.common.model;

/**
 * Lifecycle states for an {@link ArbitrageOpportunity}.
 *
 * <p>State transitions:
 * <pre>
 *   DETECTED → OPEN → CLOSED
 *                   → EXPIRED (if open longer than MAX_TRACKING_DURATION)
 * </pre>
 *
 * <p>Without lifecycle tracking, every tick with a positive spread would generate
 * a separate event. At 100ms tick rate across 3 exchanges, that's thousands of
 * duplicate events per minute. The state machine deduplicates and tracks duration.
 */
public enum OpportunityStatus {

    /** First tick where netSpread > 0. Initial detection event published. */
    DETECTED,

    /** Subsequent ticks where netSpread remains > 0. Duration and peak tracked. */
    OPEN,

    /** First tick where netSpread ≤ 0 after being OPEN. Final event published. */
    CLOSED,

    /** OPEN for longer than MAX_TRACKING_DURATION. Likely a data artefact. */
    EXPIRED
}
