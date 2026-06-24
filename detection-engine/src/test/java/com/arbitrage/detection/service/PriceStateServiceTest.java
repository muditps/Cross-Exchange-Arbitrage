package com.arbitrage.detection.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.model.PriceState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PriceStateService}.
 *
 * <p>Redis interactions are fully mocked — no running Redis instance required.
 * A real {@link ObjectMapper} is used so JSON round-trips exercise actual
 * serialisation / deserialisation rather than mocked behaviour.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Key format: {@code price:{exchange}:{pair}} with lowercase exchange name</li>
 *   <li>Write path: {@code SET key json PX ttl} — one atomic command (was HSET + EXPIRE)</li>
 *   <li>Read path: one {@code MGET key1 key2 key3} — all 3 exchanges in one command (was 3× HGETALL)</li>
 *   <li>Deserialisation: BigDecimal and Instant reconstructed correctly via Jackson + @Jacksonized builder</li>
 *   <li>Null handling: null entries in MGET result (expired keys) are silently omitted</li>
 *   <li>Error propagation: Redis errors surface as Mono errors, not silent failures</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class PriceStateServiceTest {

    @Mock
    private ReactiveStringRedisTemplate redisTemplate;

    @Mock
    private ReactiveValueOperations<String, String> valueOps;

    private PriceStateService service;
    private DetectionProperties properties;

    // Real ObjectMapper matching Spring Boot default config: ISO-8601 Instants, no timestamps
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private static final TradingPair BTC_USDT = TradingPair.fromSymbol("BTC-USDT");
    private static final TradingPair ETH_USDT = TradingPair.fromSymbol("ETH-USDT");

    @BeforeEach
    void setUp() {
        properties = new DetectionProperties();
        service = new PriceStateService(redisTemplate, properties, objectMapper);
        // lenient() allows this stub to go unused in buildKey tests which don't touch Redis
        lenient().doReturn(valueOps).when(redisTemplate).opsForValue();
    }

    // ─── buildKey tests ───────────────────────────────────────────────────────

    @Test
    @DisplayName("buildKey formats BINANCE key as price:binance:BTC-USDT (lowercase exchange)")
    void buildKey_binance_formatsCorrectly() {
        assertThat(service.buildKey(ExchangeId.BINANCE, BTC_USDT))
                .isEqualTo("price:binance:BTC-USDT");
    }

    @Test
    @DisplayName("buildKey formats BYBIT key as price:bybit:ETH-USDT")
    void buildKey_bybit_formatsCorrectly() {
        assertThat(service.buildKey(ExchangeId.BYBIT, ETH_USDT))
                .isEqualTo("price:bybit:ETH-USDT");
    }

    @Test
    @DisplayName("buildKey formats KUCOIN key as price:kucoin:BTC-USDT")
    void buildKey_kucoin_formatsCorrectly() {
        assertThat(service.buildKey(ExchangeId.KUCOIN, BTC_USDT))
                .isEqualTo("price:kucoin:BTC-USDT");
    }

    // ─── storeTick tests ──────────────────────────────────────────────────────

    @Test
    @DisplayName("storeTick calls SET with the correct Redis key")
    void storeTick_callsSet_withCorrectKey() {
        NormalisedTick tick = buildTick(ExchangeId.BINANCE, BTC_USDT, "40000", "40001");
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(Boolean.TRUE));

        service.storeTick(tick).block();

        verify(valueOps).set(eq("price:binance:BTC-USDT"), anyString(), any(Duration.class));
    }

    @Test
    @DisplayName("storeTick stores valid JSON containing all price and timestamp fields")
    void storeTick_storesValidJson_withAllPriceFields() throws Exception {
        NormalisedTick tick = buildTick(ExchangeId.BINANCE, BTC_USDT, "40000.12345678", "40001.87654321");
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(Boolean.TRUE));

        service.storeTick(tick).block();

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(anyString(), jsonCaptor.capture(), any(Duration.class));

        PriceState stored = objectMapper.readValue(jsonCaptor.getValue(), PriceState.class);
        assertThat(stored.getBestBidPrice()).isEqualByComparingTo(new BigDecimal("40000.12345678"));
        assertThat(stored.getBestAskPrice()).isEqualByComparingTo(new BigDecimal("40001.87654321"));
        assertThat(stored.getBestBidQuantity()).isEqualByComparingTo(BigDecimal.ONE);
        assertThat(stored.getBestAskQuantity()).isEqualByComparingTo(BigDecimal.ONE);
        assertThat(stored.getReceivedTimestamp()).isEqualTo(tick.getReceivedTimestamp());
        assertThat(stored.getProcessedTimestamp()).isEqualTo(tick.getProcessedTimestamp());
    }

    @Test
    @DisplayName("storeTick sets TTL equal to redisPriceTtlMs from DetectionProperties")
    void storeTick_setsTtl_fromRedisPriceTtlMs() {
        properties.setRedisPriceTtlMs(7_500L);
        NormalisedTick tick = buildTick(ExchangeId.BYBIT, BTC_USDT, "40000", "40002");
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(Boolean.TRUE));

        service.storeTick(tick).block();

        ArgumentCaptor<Duration> ttlCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(valueOps).set(eq("price:bybit:BTC-USDT"), anyString(), ttlCaptor.capture());
        assertThat(ttlCaptor.getValue()).isEqualTo(Duration.ofMillis(7_500));
    }

    @Test
    @DisplayName("storeTick returns true when SET succeeds")
    void storeTick_returnsTrue_whenSetSucceeds() {
        NormalisedTick tick = buildTick(ExchangeId.KUCOIN, BTC_USDT, "40000", "40003");
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(Boolean.TRUE));

        StepVerifier.create(service.storeTick(tick))
                .expectNext(Boolean.TRUE)
                .verifyComplete();
    }

    @Test
    @DisplayName("storeTick propagates SET error downstream")
    void storeTick_propagatesError_onSetFailure() {
        NormalisedTick tick = buildTick(ExchangeId.BINANCE, BTC_USDT, "40000", "40001");
        when(valueOps.set(anyString(), anyString(), any(Duration.class)))
                .thenReturn(Mono.error(new RuntimeException("Redis SET failed")));

        StepVerifier.create(service.storeTick(tick))
                .expectErrorMessage("Redis SET failed")
                .verify();
    }

    // ─── getAllPricesForPair tests ─────────────────────────────────────────────

    @Test
    @DisplayName("getAllPricesForPair calls MGET exactly once with all three exchange keys")
    void getAllPricesForPair_callsMget_once_withAllThreeKeys() {
        when(valueOps.multiGet(anyList())).thenReturn(Mono.just(Arrays.asList(null, null, null)));

        service.getAllPricesForPair(BTC_USDT).block();

        ArgumentCaptor<Collection<String>> keysCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(valueOps, times(1)).multiGet(keysCaptor.capture());
        assertThat(keysCaptor.getValue()).containsExactlyInAnyOrder(
                "price:binance:BTC-USDT", "price:bybit:BTC-USDT", "price:kucoin:BTC-USDT");
    }

    @Test
    @DisplayName("getAllPricesForPair returns PriceState for all three exchanges when all have live prices")
    void getAllPricesForPair_returnsAllThree_whenAllPresent() throws Exception {
        String json = buildJson("40000", "40001", "1.5", "2.0",
                Instant.now().toEpochMilli(), System.nanoTime(), System.nanoTime());

        when(valueOps.multiGet(anyList())).thenReturn(Mono.just(List.of(json, json, json)));

        Map<ExchangeId, PriceState> result = service.getAllPricesForPair(BTC_USDT).block();

        assertThat(result).hasSize(3);
        assertThat(result).containsKeys(ExchangeId.BINANCE, ExchangeId.BYBIT, ExchangeId.KUCOIN);
    }

    @Test
    @DisplayName("getAllPricesForPair deserialises BigDecimal prices correctly from JSON (8 dp precision)")
    void getAllPricesForPair_deserialisesJsonCorrectly() throws Exception {
        long epochMs = Instant.now().toEpochMilli();
        String json = buildJson("67250.50000000", "67251.25000000", "0.12345678", "0.87654321",
                epochMs, 1_000_000L, 1_001_000L);

        when(valueOps.multiGet(anyList()))
                .thenReturn(Mono.just(Arrays.asList(json, null, null)));

        PriceState state = service.getAllPricesForPair(BTC_USDT).block().get(ExchangeId.BINANCE);

        assertThat(state.getBestBidPrice()).isEqualByComparingTo(new BigDecimal("67250.50000000"));
        assertThat(state.getBestAskPrice()).isEqualByComparingTo(new BigDecimal("67251.25000000"));
        assertThat(state.getBestBidQuantity()).isEqualByComparingTo(new BigDecimal("0.12345678"));
        assertThat(state.getBestAskQuantity()).isEqualByComparingTo(new BigDecimal("0.87654321"));
        assertThat(state.getReceivedTimestamp()).isEqualTo(1_000_000L);
        assertThat(state.getExchangeTimestamp()).isEqualTo(Instant.ofEpochMilli(epochMs));
    }

    @Test
    @DisplayName("getAllPricesForPair omits exchanges whose MGET value is null (expired key)")
    void getAllPricesForPair_omitsNullEntries() throws Exception {
        String json = buildJson("40000", "40001", "1.0", "1.0",
                Instant.now().toEpochMilli(), System.nanoTime(), System.nanoTime());

        // BINANCE present, BYBIT + KUCOIN expired (null in MGET result)
        when(valueOps.multiGet(anyList()))
                .thenReturn(Mono.just(Arrays.asList(json, null, null)));

        Map<ExchangeId, PriceState> result = service.getAllPricesForPair(BTC_USDT).block();

        assertThat(result).hasSize(1);
        assertThat(result).containsKey(ExchangeId.BINANCE);
        assertThat(result).doesNotContainKey(ExchangeId.BYBIT);
        assertThat(result).doesNotContainKey(ExchangeId.KUCOIN);
    }

    @Test
    @DisplayName("getAllPricesForPair returns empty map when all MGET values are null")
    void getAllPricesForPair_returnsEmptyMap_whenAllNull() {
        when(valueOps.multiGet(anyList())).thenReturn(Mono.just(Arrays.asList(null, null, null)));

        Map<ExchangeId, PriceState> result = service.getAllPricesForPair(BTC_USDT).block();

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getAllPricesForPair propagates Redis MGET error downstream")
    void getAllPricesForPair_propagatesError_onMgetFailure() {
        when(valueOps.multiGet(anyList()))
                .thenReturn(Mono.error(new RuntimeException("Redis MGET timeout")));

        StepVerifier.create(service.getAllPricesForPair(BTC_USDT))
                .expectErrorMessage("Redis MGET timeout")
                .verify();
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private NormalisedTick buildTick(ExchangeId exchange, TradingPair pair,
                                     String bidPrice, String askPrice) {
        return NormalisedTick.builder()
                .exchangeId(exchange)
                .tradingPair(pair)
                .bestBidPrice(new BigDecimal(bidPrice))
                .bestAskPrice(new BigDecimal(askPrice))
                .bestBidQuantity(BigDecimal.ONE)
                .bestAskQuantity(BigDecimal.ONE)
                .exchangeTimestamp(Instant.now())
                .receivedTimestamp(System.nanoTime())
                .processedTimestamp(System.nanoTime())
                .build();
    }

    private String buildJson(String bid, String ask, String bidQty, String askQty,
                              long exchangeEpochMs, long receivedNano, long processedNano)
            throws Exception {
        return objectMapper.writeValueAsString(
                PriceState.builder()
                        .bestBidPrice(new BigDecimal(bid))
                        .bestAskPrice(new BigDecimal(ask))
                        .bestBidQuantity(new BigDecimal(bidQty))
                        .bestAskQuantity(new BigDecimal(askQty))
                        .exchangeTimestamp(Instant.ofEpochMilli(exchangeEpochMs))
                        .receivedTimestamp(receivedNano)
                        .processedTimestamp(processedNano)
                        .build());
    }
}
