package com.arbitrage.common.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;

/**
 * Utility for reading and writing nanosecond timestamps as Kafka message headers.
 *
 * <p>Timestamps are encoded as 8-byte big-endian longs — minimal wire overhead
 * (80 bytes for all 10 headers per message). Using headers keeps the domain models
 * ({@link com.arbitrage.common.model.NormalisedTick},
 * {@link com.arbitrage.common.model.ArbitrageOpportunity}) free of instrumentation
 * concerns: a consumer that does not care about latency ignores the headers entirely.
 *
 * <p><b>Missing-header contract:</b> {@link #read(Headers, String)} returns {@code 0L}
 * when the header is absent. This allows mixed deployments — messages produced before
 * instrumentation was added are processed without error; the LatencyContext simply shows
 * zeros for unavailable stages.
 *
 * <p><b>Header key naming:</b> {@code lat.tN} where N is the stage index (0–9).
 * Short keys minimise per-message overhead. The {@code lat.} prefix avoids collision
 * with Kafka's internal headers (which use {@code __}).
 */
public final class KafkaHeaderUtils {

    /** T0: {@code System.nanoTime()} when WebSocket {@code onMessage} fires in the connector. */
    public static final String HDR_T0 = "lat.t0";

    /** T1: {@code System.nanoTime()} when JSON parsing begins in the connector. */
    public static final String HDR_T1 = "lat.t1";

    /** T2: {@code System.nanoTime()} just before the raw tick is sent to Kafka. */
    public static final String HDR_T2 = "lat.t2";

    /** T3: {@code System.nanoTime()} when the normalisation consumer polls the message. */
    public static final String HDR_T3 = "lat.t3";

    /** T4: {@code System.nanoTime()} when the normalisation transform completes. */
    public static final String HDR_T4 = "lat.t4";

    /** T5: {@code System.nanoTime()} just before the normalised tick is sent to Kafka. */
    public static final String HDR_T5 = "lat.t5";

    /** T6: {@code System.nanoTime()} when the detection consumer polls the message. */
    public static final String HDR_T6 = "lat.t6";

    /** T7: {@code System.nanoTime()} after Redis price state is updated. */
    public static final String HDR_T7 = "lat.t7";

    /** T8: {@code System.nanoTime()} after the comparison logic completes. */
    public static final String HDR_T8 = "lat.t8";

    /** T9: {@code System.nanoTime()} just before the opportunity event is sent to Kafka. */
    public static final String HDR_T9 = "lat.t9";

    private KafkaHeaderUtils() {}

    /**
     * Encodes a {@code long} as an 8-byte big-endian array.
     *
     * @param value the nanosecond timestamp to encode
     * @return 8-byte big-endian representation
     */
    public static byte[] encode(long value) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(value);
        return buf.array();
    }

    /**
     * Decodes an 8-byte big-endian array back to a {@code long}.
     *
     * @param bytes the encoded timestamp; must be exactly 8 bytes
     * @return the decoded nanosecond timestamp
     */
    public static long decode(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * Reads a nanosecond timestamp from a Kafka header, returning {@code 0L} if absent.
     *
     * <p>Uses {@link Headers#lastHeader(String)} because duplicate headers (from retries
     * or bugs) should resolve to the most recent write.
     *
     * @param headers Kafka record headers
     * @param key     the header key (one of the {@code HDR_*} constants)
     * @return the decoded timestamp, or {@code 0L} if the header is missing
     */
    public static long read(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header != null ? decode(header.value()) : 0L;
    }

    /**
     * Writes (or overwrites) a nanosecond timestamp into the Kafka headers.
     *
     * <p>Removes any existing header with the same key before adding the new one,
     * preventing duplicate entries across pipeline stages.
     *
     * @param headers the mutable Kafka record headers
     * @param key     the header key (one of the {@code HDR_*} constants)
     * @param nanos   the nanosecond timestamp to write
     */
    public static void write(Headers headers, String key, long nanos) {
        headers.remove(key);
        headers.add(key, encode(nanos));
    }
}
