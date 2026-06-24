package com.arbitrage.detection.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link GcStatsService}.
 *
 * <p>These tests exercise the live JVM MXBean API — no mocks, no Spring context.
 * The assertions verify structural correctness (non-null, sane values) rather than
 * fixed numbers, since actual GC counts depend on JVM startup activity.
 */
class GcStatsServiceTest {

    private final GcStatsService service = new GcStatsService();

    @Test
    void captureSnapshot_returnsNonNull() {
        assertThat(service.captureSnapshot()).isNotNull();
    }

    @Test
    void captureSnapshot_heapFieldsArePositive() {
        GcSnapshot snapshot = service.captureSnapshot();
        assertThat(snapshot.heapUsedMb()).isGreaterThan(0);
        assertThat(snapshot.heapMaxMb()).isGreaterThan(0);
        assertThat(snapshot.heapUsedMb()).isLessThanOrEqualTo(snapshot.heapMaxMb());
    }

    @Test
    void captureSnapshot_gcTypeIsKnown() {
        GcSnapshot snapshot = service.captureSnapshot();
        // JDK 21 always ships with a named GC; "Unknown" would indicate a new/unrecognised GC
        assertThat(snapshot.gcType()).isNotBlank();
        assertThat(snapshot.gcType()).isNotEqualTo("Unknown");
    }

    @Test
    void captureSnapshot_gcBeansNotEmpty() {
        GcSnapshot snapshot = service.captureSnapshot();
        assertThat(snapshot.gcBeans()).isNotEmpty();
        snapshot.gcBeans().forEach(bean -> assertThat(bean.name()).isNotBlank());
    }

    @Test
    void captureSnapshot_jvmVersionNotBlank() {
        GcSnapshot snapshot = service.captureSnapshot();
        assertThat(snapshot.jvmVersion()).isNotBlank();
    }

    @Test
    void totalGcCount_isNonNegative() {
        GcSnapshot snapshot = service.captureSnapshot();
        assertThat(snapshot.totalGcCount()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void captureSnapshot_capturedAtMs_isReasonablyRecent() {
        long before = System.currentTimeMillis();
        GcSnapshot snapshot = service.captureSnapshot();
        long after = System.currentTimeMillis();
        assertThat(snapshot.capturedAtMs()).isBetween(before, after);
    }
}
