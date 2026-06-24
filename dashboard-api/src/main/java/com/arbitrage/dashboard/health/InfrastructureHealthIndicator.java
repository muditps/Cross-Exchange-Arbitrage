package com.arbitrage.dashboard.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Custom health indicator that checks connectivity to all infrastructure dependencies:
 * Kafka, Redis, and TimescaleDB.
 *
 * <p>Spring Actuator aggregates all {@link HealthIndicator} implementations. If ANY
 * indicator reports DOWN, the overall system health is DOWN. This is the composite
 * health pattern — the system is only as healthy as its weakest dependency.
 *
 * <p><strong>Why this matters for trading systems:</strong> In production, the health
 * endpoint is checked by load balancers and monitoring systems. A DOWN status prevents
 * traffic from being routed to this instance and triggers ops team alerts. You never
 * want to start processing market data if Redis (your price state store) is unreachable
 * — you'd make detection decisions with missing data.
 *
 * <p><strong>How it works:</strong> Each dependency check is a lightweight connectivity
 * test — a Kafka metadata fetch, a Redis PING, and a SQL query. These are designed to
 * be fast (< 5 seconds combined) and non-disruptive. They test connectivity, not
 * performance.
 *
 * @see org.springframework.boot.actuate.health.HealthIndicator
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class InfrastructureHealthIndicator implements HealthIndicator {

    private static final int KAFKA_CONNECTION_TIMEOUT_MS = 3000;
    private static final String HEALTH_CHECK_SQL = "SELECT 1";

    private final ReactiveRedisConnectionFactory redisConnectionFactory;
    private final DataSource dataSource;
    private final org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;

    /**
     * Checks the health of all infrastructure dependencies and returns an aggregate result.
     *
     * <p>Each dependency is checked independently — if Kafka is down but Redis and
     * TimescaleDB are up, you'll see exactly which one failed and why. This granularity
     * is essential for fast incident diagnosis.
     *
     * @return Health status with per-dependency details
     */
    @Override
    public Health health() {
        final Map<String, Object> details = new HashMap<>();

        final boolean isKafkaHealthy = checkKafkaHealth(details);
        final boolean isRedisHealthy = checkRedisHealth(details);
        final boolean isTimescaleDbHealthy = checkTimescaleDbHealth(details);

        if (isKafkaHealthy && isRedisHealthy && isTimescaleDbHealthy) {
            return Health.up()
                    .withDetails(details)
                    .build();
        }

        return Health.down()
                .withDetails(details)
                .build();
    }

    /**
     * Checks Kafka connectivity by creating a short-lived AdminClient and fetching
     * the cluster ID. If the cluster responds within the timeout, Kafka is healthy.
     *
     * <p><strong>Why AdminClient instead of a producer/consumer?</strong> AdminClient
     * is lightweight — it only fetches metadata, doesn't produce or consume messages.
     * It's the equivalent of a database "SELECT 1" — minimum work to verify connectivity.
     *
     * @param details map to populate with Kafka health details
     * @return true if Kafka is reachable, false otherwise
     */
    private boolean checkKafkaHealth(Map<String, Object> details) {
        final Properties adminProperties = new Properties();
        adminProperties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafkaProperties.getBootstrapServers())
        );
        adminProperties.put(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                KAFKA_CONNECTION_TIMEOUT_MS
        );
        adminProperties.put(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                KAFKA_CONNECTION_TIMEOUT_MS
        );

        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            final String clusterId = adminClient.describeCluster()
                    .clusterId()
                    .get(KAFKA_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            details.put("kafka", "UP");
            details.put("kafka.clusterId", clusterId);
            details.put("kafka.bootstrapServers", String.join(",", kafkaProperties.getBootstrapServers()));
            log.debug("Kafka health check passed: clusterId={}", clusterId);
            return true;

        } catch (Exception exception) {
            details.put("kafka", "DOWN");
            details.put("kafka.error", exception.getMessage());
            log.warn("Kafka health check failed: error={}", exception.getMessage());
            return false;
        }
    }

    /**
     * Checks Redis connectivity by sending a PING command through the reactive
     * connection factory. Redis PING returns PONG if the server is reachable.
     *
     * <p><strong>Why block here?</strong> Health checks are synchronous by design
     * (the {@link HealthIndicator} interface returns {@link Health}, not
     * {@code Mono<Health>}). We use {@code block()} with a timeout to bridge the
     * reactive Redis client to the synchronous health check contract. This is
     * acceptable because health checks run infrequently (every few seconds), not
     * on the hot path.
     *
     * @param details map to populate with Redis health details
     * @return true if Redis is reachable, false otherwise
     */
    private boolean checkRedisHealth(Map<String, Object> details) {
        try {
            final String pongResponse = redisConnectionFactory
                    .getReactiveConnection()
                    .ping()
                    .block(java.time.Duration.ofSeconds(2));

            if ("PONG".equals(pongResponse)) {
                details.put("redis", "UP");
                log.debug("Redis health check passed");
                return true;
            }

            details.put("redis", "DOWN");
            details.put("redis.error", "Unexpected PING response: " + pongResponse);
            return false;

        } catch (Exception exception) {
            details.put("redis", "DOWN");
            details.put("redis.error", exception.getMessage());
            log.warn("Redis health check failed: error={}", exception.getMessage());
            return false;
        }
    }

    /**
     * Checks TimescaleDB connectivity by executing a simple SQL query through the
     * JDBC DataSource. {@code SELECT 1} is the lightest possible query — it doesn't
     * touch any table, just verifies the connection is alive and the database can
     * process a request.
     *
     * @param details map to populate with TimescaleDB health details
     * @return true if TimescaleDB is reachable, false otherwise
     */
    private boolean checkTimescaleDbHealth(Map<String, Object> details) {
        try (Connection connection = dataSource.getConnection()) {
            connection.createStatement().execute(HEALTH_CHECK_SQL);

            details.put("timescaledb", "UP");
            details.put("timescaledb.database", connection.getCatalog());
            log.debug("TimescaleDB health check passed: database={}", connection.getCatalog());
            return true;

        } catch (SQLException exception) {
            details.put("timescaledb", "DOWN");
            details.put("timescaledb.error", exception.getMessage());
            log.warn("TimescaleDB health check failed: error={}", exception.getMessage());
            return false;
        }
    }
}
