package com.arbitrage.detection.service;

import com.arbitrage.common.model.ExchangeId;
import com.arbitrage.common.model.NormalisedTick;
import com.arbitrage.common.model.TradingPair;
import com.arbitrage.detection.config.DetectionProperties;
import com.arbitrage.detection.model.PriceState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Manages per-exchange price state in Redis for the detection engine.
 *
 * <p>Each incoming {@link NormalisedTick} is serialised to JSON and stored as a Redis
 * String under the key {@code price:{exchange}:{pair}} (e.g., {@code price:binance:BTC-USDT}).
 * A TTL is set atomically on the same command — if the key expires, the price is definitionally
 * stale and auto-evicted.
 *
 * <p><b>Write path — 1 Redis command per tick:</b>
 * {@link #storeTick} issues {@code SET price:exchange:pair "{json}" PX ttl} — a single
 * atomic command that writes the JSON value and sets the expiry simultaneously.
 * The previous approach ({@code HSET} + {@code EXPIRE}) required 2 sequential commands
 * with a window where the key could exist without a TTL if {@code EXPIRE} failed.
 *
 * <p><b>Read path — 1 Redis command for all exchanges:</b>
 * {@link #getAllPricesForPair} issues one {@code MGET key1 key2 key3} command to fetch
 * all 3 exchange prices in a single round-trip. The previous approach issued 3 concurrent
 * {@code HGETALL} commands — still 3 commands on the Redis server even if pipelined.
 * Null entries in the {@code MGET} result indicate an expired or absent key and are silently
 * omitted from the result map.
 *
 * <p><b>Combined optimisation (Session 6.4):</b> 5 Redis commands per tick cycle → 2.
 * Expected impact: {@code DETECTION_PROCESSING} p99 ~7ms → target ~1ms.
 *
 * <p><b>BigDecimal precision:</b> Jackson serialises {@code BigDecimal} fields as JSON numbers
 * and deserialises them back to {@code BigDecimal} when the target field type is declared as
 * {@code BigDecimal} — no floating-point intermediate conversion. {@link PriceState} carries
 * {@code @Jacksonized} so the Lombok builder is used for deserialisation, satisfying Jackson's
 * requirement for an immutable value type with no public no-arg constructor.
 *
 * <p><b>Two time thresholds (do not confuse):</b>
 * <ul>
 *   <li>Redis TTL ({@link DetectionProperties#getRedisPriceTtlMs()}, 10 s default) — auto-eviction backstop.</li>
 *   <li>Staleness threshold ({@link DetectionProperties#getStalenessThresholdMs()}, 500 ms default) —
 *       comparison-time filter checked by detection logic.</li>
 * </ul>
 *
 * <p><b>Why {@link EnumMap}?</b> {@link ExchangeId} is a small, fixed-size enum.
 * {@link EnumMap} is backed by a direct array indexed by enum ordinal — O(1) access
 * with zero boxing overhead vs {@link java.util.HashMap}.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PriceStateService {

    /** Redis key prefix for all price state entries. */
    static final String KEY_PREFIX = "price";

    private final ReactiveStringRedisTemplate redisTemplate;
    private final DetectionProperties detectionProperties;
    private final ObjectMapper objectMapper;

    /**
     * Stores a normalised tick as a JSON string in Redis with an atomic TTL.
     *
     * <p>Uses Redis {@code SET key value PX ttl} — writes value and TTL in one command,
     * removing the two-step {@code HSET} + {@code EXPIRE} race from the previous approach.
     *
     * @param tick the normalised tick to store
     * @return {@code Mono<Boolean>} emitting {@code true} on success; propagates any error
     */
    public Mono<Boolean> storeTick(NormalisedTick tick) {
        String key = buildKey(tick.getExchangeId(), tick.getTradingPair());
        PriceState state = buildPriceState(tick);
        Duration ttl = Duration.ofMillis(detectionProperties.getRedisPriceTtlMs());

        return Mono.fromCallable(() -> objectMapper.writeValueAsString(state))
                .flatMap(json -> redisTemplate.opsForValue().set(key, json, ttl))
                .thenReturn(Boolean.TRUE)
                .doOnSuccess(stored -> log.debug(
                        "Stored tick: key={} exchange={} pair={} ttl={}ms",
                        key, tick.getExchangeId(), tick.getTradingPair().canonicalSymbol(),
                        detectionProperties.getRedisPriceTtlMs()))
                .doOnError(ex -> log.error(
                        "Failed to store tick: key={} exchange={} error={}",
                        key, tick.getExchangeId(), ex.getMessage()));
    }

    /**
     * Retrieves the current live price for a trading pair from all exchanges via a single
     * Redis {@code MGET} command.
     *
     * <p>Keys are built in {@link ExchangeId} enum ordinal order, and the result list
     * preserves that order — allowing a simple index-based zip back to exchange identifiers.
     * Null entries in the {@code MGET} response (expired or absent keys) are skipped.
     *
     * @param pair the trading pair to retrieve prices for
     * @return {@code Mono<Map<ExchangeId, PriceState>>} — live prices only, never null values;
     *         may be empty if all prices are absent
     */
    public Mono<Map<ExchangeId, PriceState>> getAllPricesForPair(TradingPair pair) {
        ExchangeId[] exchanges = ExchangeId.values();
        List<String> keys = Arrays.stream(exchanges)
                .map(id -> buildKey(id, pair))
                .toList();

        return redisTemplate.opsForValue()
                .multiGet(keys)
                .flatMap(values -> {
                    Map<ExchangeId, PriceState> result = new EnumMap<>(ExchangeId.class);
                    for (int i = 0; i < exchanges.length; i++) {
                        String json = values.get(i);
                        if (json == null) {
                            continue;
                        }
                        try {
                            result.put(exchanges[i], objectMapper.readValue(json, PriceState.class));
                        } catch (JsonProcessingException ex) {
                            log.warn("Malformed price state in Redis: exchange={} pair={} error={} — skipping",
                                    exchanges[i], pair.canonicalSymbol(), ex.getMessage());
                        }
                    }
                    return Mono.just(result);
                })
                .doOnSuccess(map -> log.debug(
                        "Fetched prices: pair={} liveExchanges={}",
                        pair.canonicalSymbol(), map.keySet()))
                .doOnError(ex -> log.error(
                        "Failed to fetch prices: pair={} error={}",
                        pair.canonicalSymbol(), ex.getMessage()));
    }

    /**
     * Builds the Redis key for a given exchange and trading pair.
     *
     * <p>Format: {@code price:{exchange}:{CANONICAL_SYMBOL}}
     * <br>Example: {@code price:binance:BTC-USDT}
     *
     * @param exchangeId  the exchange identifier
     * @param tradingPair the trading pair
     * @return the Redis key string
     */
    String buildKey(ExchangeId exchangeId, TradingPair tradingPair) {
        return KEY_PREFIX + ":" + exchangeId.name().toLowerCase() + ":" + tradingPair.canonicalSymbol();
    }

    private PriceState buildPriceState(NormalisedTick tick) {
        return PriceState.builder()
                .bestBidPrice(tick.getBestBidPrice())
                .bestAskPrice(tick.getBestAskPrice())
                .bestBidQuantity(tick.getBestBidQuantity())
                .bestAskQuantity(tick.getBestAskQuantity())
                .exchangeTimestamp(tick.getExchangeTimestamp())
                .receivedTimestamp(tick.getReceivedTimestamp())
                .processedTimestamp(tick.getProcessedTimestamp())
                .build();
    }
}
