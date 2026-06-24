package com.arbitrage.common.util;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit tests for {@link KafkaHeaderUtils}.
 *
 * <p>Verifies encode/decode round-trip correctness, write/read through a real
 * {@link Headers} instance, and the default-zero contract for missing headers.
 */
@DisplayName("KafkaHeaderUtils")
class KafkaHeaderUtilsTest {

    @Test
    @DisplayName("encode then decode round-trips positive nanosecond values")
    void encodeDecodeRoundTrip_positiveValue() {
        long value = 1_718_000_000_123_456_789L;

        assertEquals(value, KafkaHeaderUtils.decode(KafkaHeaderUtils.encode(value)));
    }

    @Test
    @DisplayName("encode then decode round-trips zero")
    void encodeDecodeRoundTrip_zero() {
        assertEquals(0L, KafkaHeaderUtils.decode(KafkaHeaderUtils.encode(0L)));
    }

    @Test
    @DisplayName("encode then decode round-trips Long.MAX_VALUE")
    void encodeDecodeRoundTrip_maxValue() {
        assertEquals(Long.MAX_VALUE, KafkaHeaderUtils.decode(KafkaHeaderUtils.encode(Long.MAX_VALUE)));
    }

    @Test
    @DisplayName("write then read round-trips through a Headers object")
    void writeReadRoundTrip() {
        Headers headers = new RecordHeaders();
        long nanos = 987_654_321_000L;

        KafkaHeaderUtils.write(headers, KafkaHeaderUtils.HDR_T1, nanos);

        assertEquals(nanos, KafkaHeaderUtils.read(headers, KafkaHeaderUtils.HDR_T1));
    }

    @Test
    @DisplayName("read returns 0 for missing header — backward-compatible default")
    void read_missingHeader_returnsZero() {
        Headers headers = new RecordHeaders();

        assertEquals(0L, KafkaHeaderUtils.read(headers, KafkaHeaderUtils.HDR_T0));
    }

    @Test
    @DisplayName("write overwrites an existing header value")
    void write_overwritesExistingValue() {
        Headers headers = new RecordHeaders();
        KafkaHeaderUtils.write(headers, KafkaHeaderUtils.HDR_T3, 111L);
        KafkaHeaderUtils.write(headers, KafkaHeaderUtils.HDR_T3, 999L);

        assertEquals(999L, KafkaHeaderUtils.read(headers, KafkaHeaderUtils.HDR_T3));
    }

    @Test
    @DisplayName("all T0–T9 header name constants are distinct")
    void allHeaderNames_areDistinct() {
        String[] names = {
                KafkaHeaderUtils.HDR_T0, KafkaHeaderUtils.HDR_T1, KafkaHeaderUtils.HDR_T2,
                KafkaHeaderUtils.HDR_T3, KafkaHeaderUtils.HDR_T4, KafkaHeaderUtils.HDR_T5,
                KafkaHeaderUtils.HDR_T6, KafkaHeaderUtils.HDR_T7, KafkaHeaderUtils.HDR_T8,
                KafkaHeaderUtils.HDR_T9
        };
        long distinctCount = java.util.Arrays.stream(names).distinct().count();
        assertEquals(names.length, distinctCount, "All header name constants must be distinct");
    }
}
