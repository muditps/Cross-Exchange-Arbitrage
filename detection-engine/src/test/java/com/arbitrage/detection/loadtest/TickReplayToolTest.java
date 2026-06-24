package com.arbitrage.detection.loadtest;

import com.arbitrage.detection.service.LatencyMetricsPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Map;

import static com.arbitrage.detection.config.DetectionKafkaConsumerConfig.TOPIC_NORMALISED_TICKS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TickReplayToolTest {

    @Mock
    private KafkaTemplate<String, com.arbitrage.common.model.NormalisedTick> replayKafkaTemplate;

    @Mock
    private LatencyMetricsPublisher latencyMetricsPublisher;

    private TickReplayTool tool;

    @BeforeEach
    void setUp() {
        // 0ms settle avoids the 12s post-run wait in unit tests
        tool = new TickReplayTool(replayKafkaTemplate, latencyMetricsPublisher, 0L);
        lenient().when(latencyMetricsPublisher.getLatestPercentiles()).thenReturn(buildEmptyPercentiles());
    }

    @Test
    void replayMode_realtime_hasExpected90TicksPerSecond() {
        assertThat(ReplayMode.REALTIME.getTicksPerSecond()).isEqualTo(90);
        assertThat(ReplayMode.REALTIME.isBurst()).isFalse();
    }

    @Test
    void replayMode_burst_hasBurstFlag() {
        assertThat(ReplayMode.BURST.isBurst()).isTrue();
        assertThat(ReplayMode.BURST.getTicksPerSecond()).isEqualTo(-1);
    }

    @Test
    void runLoadTest_realtimeMode_producesTicksToCorrectTopic() {
        final ReplayResult result = tool.runLoadTest(ReplayMode.REALTIME, 1);

        assertThat(result).isNotNull();
        verify(replayKafkaTemplate, atLeastOnce())
                .send(eq(TOPIC_NORMALISED_TICKS), any(), any());
        assertThat(result.mode()).isEqualTo(ReplayMode.REALTIME);
        assertThat(result.ticksProduced()).isGreaterThan(0);
    }

    @Test
    void runLoadTest_concurrentCall_returnsNull() throws InterruptedException {
        // Run a 1s injection in the background; with 0ms settle it completes in ~1s
        final Thread backgroundRun = new Thread(() -> tool.runLoadTest(ReplayMode.REALTIME, 1));
        backgroundRun.start();
        Thread.sleep(100); // let background run acquire the AtomicBoolean lock

        final ReplayResult concurrent = tool.runLoadTest(ReplayMode.REALTIME, 1);
        assertThat(concurrent).isNull();

        backgroundRun.join(3000); // background run finishes in ~1s; 3s is a safe upper bound
    }

    @Test
    void runLoadTest_realtimeMode_kafkaKeyMatchesNormalisationServiceFormat() {
        tool.runLoadTest(ReplayMode.REALTIME, 1);

        final ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(replayKafkaTemplate, atLeastOnce())
                .send(eq(TOPIC_NORMALISED_TICKS), keyCaptor.capture(), any());

        // Key format: "EXCHANGEID:PAIR" — must match NormalisationService key to ensure correct partition routing
        final List<String> keys = keyCaptor.getAllValues();
        assertThat(keys).isNotEmpty();
        for (String key : keys) {
            assertThat(key).matches("^(BINANCE|BYBIT|KUCOIN):(BTC|ETH|BNB)-USDT$");
        }
    }

    @Test
    void replayResult_of_setsPerformanceCliffDetected_whenP99CrossesThreshold() {
        final Map<String, Map<String, Long>> beforeNanos = Map.of(
                "END_TO_END", Map.of("p50", 10_000_000L, "p95", 50_000_000L, "p99", 80_000_000L, "p999", 90_000_000L)
        );
        final Map<String, Map<String, Long>> afterNanos = Map.of(
                "END_TO_END", Map.of("p50", 20_000_000L, "p95", 80_000_000L, "p99", 150_000_000L, "p999", 300_000_000L)
        );

        final ReplayResult result = ReplayResult.of(ReplayMode.FAST_10X, 60, 60_000L, 54_000L, beforeNanos, afterNanos);

        assertThat(result.endToEndP99BeforeMs()).isEqualTo(80.0);
        assertThat(result.endToEndP99AfterMs()).isEqualTo(150.0);
        assertThat(result.performanceCliffDetected()).isTrue();
    }

    @Test
    void replayResult_of_doesNotSetCliff_whenP99AlreadyAboveThresholdBefore() {
        // If p99 was already >100ms before the run, the cliff was found earlier — don't re-flag it
        final Map<String, Map<String, Long>> beforeNanos = Map.of(
                "END_TO_END", Map.of("p50", 20_000_000L, "p95", 90_000_000L, "p99", 120_000_000L, "p999", 200_000_000L)
        );
        final Map<String, Map<String, Long>> afterNanos = Map.of(
                "END_TO_END", Map.of("p50", 25_000_000L, "p95", 100_000_000L, "p99", 180_000_000L, "p999", 400_000_000L)
        );

        final ReplayResult result = ReplayResult.of(ReplayMode.FAST_10X, 60, 60_000L, 54_000L, beforeNanos, afterNanos);

        assertThat(result.performanceCliffDetected()).isFalse();
    }

    private Map<String, Map<String, Long>> buildEmptyPercentiles() {
        return Map.of(
                "END_TO_END", Map.of("p50", 0L, "p95", 0L, "p99", 0L, "p999", 0L),
                "DETECTION_PROCESSING", Map.of("p50", 0L, "p95", 0L, "p99", 0L, "p999", 0L)
        );
    }
}
