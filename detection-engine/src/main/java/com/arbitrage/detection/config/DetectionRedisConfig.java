package com.arbitrage.detection.config;

import org.springframework.context.annotation.Configuration;

/**
 * Redis configuration marker for the Detection Engine.
 *
 * <p>No custom bean definitions are required here: Spring Boot's
 * {@code ReactiveRedisAutoConfiguration} automatically creates:
 * <ul>
 *   <li>{@link org.springframework.data.redis.core.ReactiveStringRedisTemplate} —
 *       used by {@link com.arbitrage.detection.service.PriceStateService} for all
 *       hash operations. Uses {@code StringRedisSerializer} for both keys and values,
 *       keeping all Redis keys and field values human-readable.</li>
 *   <li>{@link org.springframework.data.redis.connection.ReactiveRedisConnectionFactory} —
 *       Lettuce-backed non-blocking connection pool.</li>
 * </ul>
 *
 * <p>Redis connection settings ({@code host}, {@code port}, pool size) are
 * configured via {@code spring.data.redis.*} properties in {@code dashboard-api/application.yml}.
 * This module is a library — it inherits the parent application's Redis connection at runtime.
 *
 * <p>If a custom serializer or connection pool configuration is needed in the future,
 * add beans here with {@code @Bean @ConditionalOnMissingBean} to remain override-friendly.
 */
@Configuration
public class DetectionRedisConfig {
    // Spring Boot ReactiveRedisAutoConfiguration provides ReactiveStringRedisTemplate
}
