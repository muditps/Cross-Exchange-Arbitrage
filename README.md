# Multi-Asset Cross-Exchange Arbitrage Detection Platform

A real-time system that detects arbitrage opportunities across multiple financial exchanges by monitoring price feeds, normalising data into a unified format, comparing prices across exchanges, and simulating trade execution — all displayed on a live dashboard.

**Starting with:** Cryptocurrency exchanges (Binance, Bybit, KuCoin) using free public WebSocket APIs.
**Extending to:** Indian equities (NSE, BSE) in Phase 7, proving the platform is truly multi-asset.

---

## Architecture

```
Exchange WebSocket Feeds
        │
        ▼
┌───────────────────┐     Kafka      ┌──────────────────┐     Kafka      ┌─────────────────┐
│ Exchange Connectors│ ──────────────▶│ Normalisation    │ ──────────────▶│ Detection Engine│
│ (per exchange)     │  raw ticks     │ Engine           │  normalised    │ (Redis state)   │
└───────────────────┘                 └──────────────────┘  ticks         └────────┬────────┘
                                                                                   │
                                                          Kafka: opportunities     │
                                                                                   ▼
┌───────────────────┐                 ┌──────────────────┐                ┌─────────────────┐
│   Dashboard UI     │◀──WebSocket───│   Dashboard API   │◀──────────────│ Execution       │
│   (React SPA)      │               │   (Spring Boot)   │               │ Simulator       │
└───────────────────┘                 └──────────────────┘                └─────────────────┘
                                              │                                    │
                                              ▼                                    ▼
                                     ┌──────────────────┐                ┌─────────────────┐
                                     │   Prometheus +    │                │  TimescaleDB    │
                                     │   Grafana         │                │  (historical)   │
                                     └──────────────────┘                └─────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Java 21, Spring Boot 3.4.x, WebFlux (reactive) |
| Message Broker | Apache Kafka (KRaft mode, no Zookeeper) |
| Price State | Redis 7 (sub-millisecond reads) |
| Historical DB | TimescaleDB (PostgreSQL + time-series extension) |
| Frontend | React 18, TypeScript 5, Vite 8, TailwindCSS 3 |
| Charting | Recharts |
| Monitoring | Micrometer + Prometheus + Grafana |
| Latency | HdrHistogram (percentile-accurate latency recording) |
| Build | Gradle 8.x (Kotlin DSL, version catalog) |
| Containers | Docker + Docker Compose |

---

## Quick Start

### Prerequisites
- **Java 21 JDK** (If you do not have it, install Eclipse Temurin 21 via `winget install EclipseAdoptium.Temurin.21.JDK`)
- **Docker Desktop** (Must be running)
- **Node.js 18+** & **npm** (For the frontend dashboard-ui)
- **Git**

### Windows System Prerequisites
1. **Create Temp Folder**: The project's `gradle.properties` references `C:/temp` as the Java temp directory to avoid Windows username spaces/NIO bugs. You must create this folder before compiling:
   ```powershell
   New-Item -ItemType Directory -Path C:\temp -Force
   ```
2. **Local PostgreSQL Conflict**: If you have a local PostgreSQL instance running on your host machine on port `5432`, you must map the Docker container to port `5433` to avoid collision:
   - Use `TIMESCALEDB_PORT=5433` when booting the Docker Compose stack.
   - Run the Spring Boot application pointing to `localhost:5433`.

---

### Step-by-Step Execution Guide

#### 1. Setup Environment
Copy the example environment file:
```bash
cp .env.example .env
```
*(Open `.env` and set `TIMESCALEDB_PASSWORD=arbitrage_dev_password`)*

#### 2. Start Infrastructure
Run Docker Compose (with port override if you have a local PostgreSQL service):
```cmd
# Standard run (binds to port 5432)
docker-compose up -d

# Port conflict resolution run (binds to port 5433)
set TIMESCALEDB_PORT=5433 && docker-compose up -d
```

#### 3. Start Backend Server
Run the Spring Boot application using the installed JDK 21 toolchain (with port override and optimized log level to prevent console buffer freezing):
```cmd
# Standard run (connects to port 5432)
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot
gradlew.bat :dashboard-api:bootRun

# Port conflict resolution run (connects to port 5433 with optimized logging)
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot
gradlew.bat :dashboard-api:bootRun --args="--spring.datasource.url=jdbc:postgresql://localhost:5433/arbitrage_detector --logging.level.com.arbitrage=INFO --logging.level.org.hibernate.SQL=INFO --logging.level.org.hibernate.type.descriptor.sql.BasicBinder=INFO"
```
Once booted, the backend API will be available at [http://localhost:8080](http://localhost:8080).

#### 4. Start Frontend UI
Navigate to the `dashboard-ui` directory and launch the Vite development server:
```cmd
cd dashboard-ui
npm.cmd run dev
```
Open **[http://localhost:5173/](http://localhost:5173/)** in your browser to view the live dashboard.

---

## Project Structure

```
arbitrage-detector/
├── common-models/          # Shared domain objects (NormalisedTick, TradingPair, etc.)
├── exchange-connectors/    # WebSocket clients per exchange (Strategy pattern)
├── normalisation-engine/   # Raw → unified format conversion
├── detection-engine/       # Cross-exchange comparison + opportunity lifecycle
├── execution-simulator/    # Simulated trade execution with slippage model
├── dashboard-api/          # Spring Boot app (WebSocket + REST)
├── dashboard-ui/           # React SPA (Phase 5+)
├── infra/                  # Docker/Kafka/Redis/Prometheus config
└── docs/                   # Full documentation suite
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/PROJECT.md](docs/PROJECT.md) | Full project documentation |
| [docs/ONBOARDING.md](docs/ONBOARDING.md) | New developer setup guide |
| [docs/DEVELOPMENT_PLAN.md](docs/DEVELOPMENT_PLAN.md) | 69-session implementation plan |
| [docs/CHANGES.md](docs/CHANGES.md) | Change log |
| [docs/BUILD_STEPS.md](docs/BUILD_STEPS.md) | Step-by-step recreation guide |
| [docs/LEARNINGS.md](docs/LEARNINGS.md) | Session learning journal |
| [docs/INTERVIEW_PREP.md](docs/INTERVIEW_PREP.md) | Interview question bank |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Issues + debugging journeys |
| [docs/GLOSSARY.md](docs/GLOSSARY.md) | Domain terminology |
| [docs/exchanges/BINANCE.md](docs/exchanges/BINANCE.md) | Binance API reference + gotchas |
| [docs/phase-reports/PHASE-0.md](docs/phase-reports/PHASE-0.md) | Phase 0 completion report |
| [docs/phase-reports/PHASE-1.md](docs/phase-reports/PHASE-1.md) | Phase 1 completion report |
| [docs/phase-reports/PHASE-2.md](docs/phase-reports/PHASE-2.md) | Phase 2 completion report |
| [docs/exchanges/BYBIT.md](docs/exchanges/BYBIT.md) | Bybit API reference + gotchas |
| [docs/exchanges/KUCOIN.md](docs/exchanges/KUCOIN.md) | KuCoin API reference + REST bootstrap |
| [docs/exchanges/NSE.md](docs/exchanges/NSE.md) | NSE (Angel One SmartAPI) — auth flow, paise pricing, instrument tokens, market hours |

---

## Current Status

**Phase 7A in progress** — Sessions 7A.1–7A.2 COMPLETE. NSE connector architecture (7A.1) + full message parsing pipeline (7A.2): `DataQuality` enum (`FULL_BOOK`/`LTP_ONLY`), `NseQuoteMessage` wire-format POJO, `NseTickTransformer` (paise→INR, token→pair lookup), full `NseMessageParser`. ~596 backend tests + 15 frontend tests. Phase 6: G1GC_2GB winner — DETECTION p99=11.87ms, E2E p99=159ms. **Next: Session 7A.3** — Kafka wiring (`NseTickKafkaProducer`, `raw-ticks-nse` topic, normalisation engine listener).

### Phase 1 Complete
- [x] Gradle multi-module project (6 modules)
- [x] Core domain objects (7 classes, 34 tests)
- [x] Docker Compose infrastructure (Kafka, Redis, TimescaleDB, Prometheus, Grafana)
- [x] Spring Boot health check endpoint
- [x] TimescaleDB schema + Flyway migrations
- [x] ExchangeConnector interface (Strategy Pattern)
- [x] Binance WebSocket connector with staleness detection
- [x] Binance message parser (bookTicker → NormalisedTick)
- [x] Kafka producer for raw Binance ticks
- [x] Exponential backoff reconnection with jitter
- [x] Micrometer metrics (RED pattern: 7 metrics)
- [x] Full integration test with Testcontainers Kafka

### Phase 2 Complete
- [x] **Session 2.1:** Bybit WebSocket connector (client-initiated heartbeat, nested wire format, server timestamp)
- [x] **Session 2.2:** KuCoin WebSocket connector (REST token bootstrap, `Mono.cache()`, dynamic ping interval)
- [x] **Session 2.3:** Normalisation Engine module setup (Kafka consumer group, producer, module entry point, 8 tests)
- [x] **Session 2.4:** Exchange-specific transformers (TickTransformer interface, 3 implementations, TickTransformerFactory, 59 tests)
- [x] **Session 2.5:** NormalisationService — Kafka pipeline (`@KafkaListener` consuming 3 raw topics, `TickTransformerFactory` routing, async publish to `normalised-ticks`, at-least-once offset commit at T5, RED metrics, 27 tests)
- [x] **Session 2.6:** FeedHealthMonitor — per-exchange state machine (CONNECTED→STALE→DISCONNECTED), time injection for deterministic tests, Micrometer gauge, 22 tests
- [x] **Session 2.7:** ClockSkewMonitor — EWMA rolling offset tracking, jump detection with WARN log, `normalisation.clock.skew.offset` gauge, Logback ListAppender log capture in tests, 16 tests
- [x] **Session 2.8:** Multi-Exchange Pipeline Integration Test — Testcontainers Kafka, full end-to-end flow from 3 raw-ticks topics through `NormalisationService` to `normalised-ticks`, 7 tests (per-exchange, all-3, key assertion, T4>T0 timestamp chain, FeedHealthMonitor CONNECTED state)
- [x] **Session 2.9:** Connector Registry + Dynamic Pair Support — `TickKafkaProducer` interface, `TradingPairsProperties` (`arbitrage.trading.pairs` YAML config), `ExchangeConnectorRegistry` (`SmartLifecycle`, `List<TickKafkaProducer>` Spring injection, `EnumMap` connector lookup), `ConnectorAutoConfiguration`. All 3 producers implement interface with per-pair `ConcurrentHashMap<String, Disposable>` subscription tracking. 17 new tests.
- [x] **Session 2.10:** Phase 2 Docs — `ADR-010-normalisation-pipeline-design.md`, `docs/phase-reports/PHASE-2.md`, 10 session evidence files in `docs/screenshots/phase-2/`

**424 tests total** (34 common-models + 251 exchange-connectors + 139 normalisation-engine). All passing.

### Phase 3 Progress
- [x] **Session 3.1:** Detection Engine module setup — `DetectionEngineApplication` (`@Configuration @ComponentScan @EnableScheduling @EnableConfigurationProperties`), `DetectionKafkaConsumerConfig` (consumer group `detection-engine-group`, `MANUAL_IMMEDIATE`, `max-poll-records=1000`, concurrency=3), `application.yml` (staleness threshold, min spread bps), 7 tests
- [x] **Session 3.2:** PriceStateService — Redis Hash Storage — `DetectionProperties` (`stalenessThresholdMs=500`, `minSpreadBps=10`, `redisPriceTtlMs=10000`), `PriceState` (@Value @Builder read model), `PriceStateService` (HSET `price:{exchange}:{pair}` with 7 fields + EXPIRE via `Mono.defer`, concurrent `HGETALL` via `Flux.flatMap`, `EnumMap` result), 20 tests
- [x] **Session 3.3:** ArbitrageDetectionEngine — Core Comparison Logic — `SpreadCalculationResult` (6-field `@Value @Builder` result), `SpreadCalculator` (pure stateless math: grossSpread/netSpread/grossSpreadBps/netSpreadBps/arbitrageableQty/theoreticalProfit — multiply-before-divide for BPS precision), `ArbitrageDetectionEngine` (`@KafkaListener` → storeTick → getAllPricesForPair → `compareAllDirections` 6 directions for 3 exchanges, noise filter + profitability filter), 19 tests (all 8 mandatory including v1.1 canonical example)
- [x] **Session 3.4:** Staleness Filtering — `StalenessFilter` (`isStale` uses `receivedTimestamp`+monotonic clock not `exchangeTimestamp`, strict `>`, ms→ns conversion, `Supplier<Long>` clock injection, `recordStaleSkip` WARN+`detection.stale.skips` Micrometer counter tagged by exchange), staleness gate wired into `ArbitrageDetectionEngine.compareAllDirections` before BigDecimal arithmetic, 14 tests (10 `StalenessFilterTest` + 3 engine staleness tests)
- [x] **Session 3.5:** Opportunity Lifecycle State Machine — `OpportunityKey` (`@Value` composite key: pair+sellExchange+buyExchange), `OpportunityTracker` (`ConcurrentHashMap` state machine: first tick→DETECTED event+OPEN; subsequent ticks→Welford mean/peak/updateCount update; absent tick→CLOSED event; `@Scheduled` expiry sweep→EXPIRED; pair-scoped closure detection; `Supplier<Long>` clock injection), `updateCount` added to `ArbitrageOpportunity`, `maxOpportunityDurationMs` added to `DetectionProperties`, 12 tests
- [x] **Session 3.6:** Opportunity Kafka Producer + Detection Metrics — `DetectionKafkaProducerConfig` (`arbitrage-opportunities` topic, `acks=1`, `linger.ms=0`), `OpportunityKafkaPublisher` (key=`pair.canonicalSymbol()`, async `whenComplete` error logging), `DetectionMetrics` (4 metrics: event counter by status, net spread bps distribution, duration ms distribution, comparison latency timer), `@Scheduled runExpiryAndPublish()` moved to engine, T6/T8 latency captured in `onNormalisedTick`, 10 tests
- [x] **Session 3.7:** Fee Configuration — `FeeConfiguration` (`@ConfigurationProperties("exchanges")`, nested `ExchangeFeeConfig` with `takerFeeRate`/`makerFeeRate`, `@Validated` + `@DecimalMin(inclusive=false)` startup validation, exhaustive switch `getTakerFeeRate(ExchangeId)`), YAML `exchanges:` block with env-var overrides for all 3 exchanges, `ArbitrageDetectionEngine` updated to use `FeeConfiguration` (4 `getDefaultTakerFeeRate()` calls replaced), 6 tests

- [x] **Session 3.8:** Phase 3 Integration Test — `DetectionPipelineIntegrationTest` (Testcontainers Kafka + Redis, end-to-end from `normalised-ticks` → detection engine → `arbitrage-opportunities`), `TestTickGenerator` (`freshTick`/`staleTick` factories), 4 tests: profitable spread emits DETECTED, net-negative spread silent, stale price silent, lifecycle DETECTED→CLOSED. Bug fix: `@Jacksonized` added to `ArbitrageOpportunity` (missing annotation caused `InvalidDefinitionException` on deserialization). +4 tests.

**516 tests total** (34 common-models + 251 exchange-connectors + 139 normalisation-engine + 92 detection-engine). All passing.

- [x] **Session 3.9:** Phase 3 Completion Docs — `docs/phase-reports/PHASE-3.md`, `docs/adr/ADR-011-detection-engine-design.md`, `docs/LEARNINGS.md` + `docs/INTERVIEW_PREP.md` updated with 5 new Q&A pairs.

**Phase 3 complete. 516 tests total. All passing.**

### Phase 4 — Execution Simulator

- [x] **Session 4.1:** Module Setup — `ExecutionSimulatorApplication` (`@Configuration @ComponentScan @EnableScheduling @EnableConfigurationProperties`), `SimulationKafkaConsumerConfig` (group: `execution-simulator-group`, `MANUAL_IMMEDIATE` ack, concurrency=3, max-poll-records=100, `setUseTypeHeaders=false`), `SimulationProperties` (`@ConfigurationProperties("arbitrage.simulation")`: `defaultSlippageBps`=2.00, `historicalWindowSeconds`=60, `enabled`), `application.yml` (Flyway disabled — dashboard-api owns migrations, JPA batch_size=25), 7 tests

- [x] **Session 4.2:** Latency Model + Timeline Simulator — `ExchangeLatencyProfile` (`@Value @Builder`, 4 components: `networkLatencyMs`/`exchangeProcessingMs`/`confirmationLatencyMs`/`jitterMs`, `totalLegLatencyMs()`), `LatencyConfiguration` (`@ConfigurationProperties("arbitrage.simulation.latency")`, nested `ExchangeLatencyConfig`, `getProfile(ExchangeId)`, defaults Binance=27ms/Bybit=43ms/KuCoin=52ms), `ExecutionTimelineSimulator` (`@Component`, formula: `detectionToDecisionMs + max(buyLeg, sellLeg)`), application.yml latency block with 12 env-var-overrideable properties, 13 tests

- [x] **Session 4.3:** Historical Price Replay Store — `PriceKey` (Java record, `ExchangeId+TradingPair` composite key), `HistoricalPriceStore` (`@Component`, `ConcurrentHashMap<PriceKey, ConcurrentLinkedDeque<NormalisedTick>>`, eviction-on-write using `exchangeTimestamp`, 60s rolling window), `NormalisedTickConsumerConfig` (`@Configuration`, `priceStoreConsumerFactory` + `priceStoreListenerContainerFactory`, group: `price-store-consumer-group`, auto-commit, max-poll-records=500), `NormalisedTickListener` (`@Component @KafkaListener` on `normalised-ticks`, separated from config class to avoid Spring CGLIB/constructor-injection/@KafkaListener `BeanCurrentlyInCreationException`), `PriceAtExecutionLookup` (`@Component`, `findClosestBefore(exchange, pair, Instant)→Optional<NormalisedTick>`, floor lookup via tail-to-head scan), 13 tests (7 store + 6 lookup)

- [x] **Session 4.4:** Slippage Estimation — `SlippageResult` (`@Value @Builder`, 6 fields: `buyExecutionPrice`, `sellExecutionPrice`, `buySlippageBps`, `sellSlippageBps`, `totalSlippageBps`, `priceDataAvailable`), `SlippageEstimator` (`@Component`, `estimate(ArbitrageOpportunity, executionLatencyMs)→SlippageResult`, buy slippage=`(executionAsk−detectionAsk)/detectionAsk×10000` bps, sell slippage=`(detectionBid−executionBid)/detectionBid×10000` bps, fallback to `defaultSlippageBps` when price store not warm), 8 tests (Mockito-mocked `PriceAtExecutionLookup`)

- [x] **Session 4.5:** Persistence Layer — `SimulationResult` (`@Entity`, `simulation_results` TimescaleDB hypertable, composite DB PK mapped as single `@Id` on UUID), `SimulationResultRepository` (`JpaRepository`, write-only), `SimulationOrchestrator` (`@Component @KafkaListener` on `arbitrage-opportunities`, CLOSED-only filter, always-acknowledge pattern, net P&L = `(sellPrice-buyPrice)*qty - fees`), `SimulationProperties` updated with `detectionToDecisionMs=5ms`, 8 tests

- [x] **Session 4.6:** End-to-End Integration Test — `SimulationPipelineIntegrationTest` (Testcontainers: `confluentinc/cp-kafka:7.6.1` + `postgres:16-alpine`, test-specific Flyway V1 migration without `create_hypertable()`, 2 tests: CLOSED→row persisted with correct slippage/latency/profitability; DETECTED→no row written). Docker fix: `systemProperty("docker.host", "tcp://localhost:2375")` + `systemProperty("api.version", "1.44")` in `execution-simulator/build.gradle.kts` (docker-java 3.4.1 defaults to API v1.41; Docker Desktop 4.64+ requires minimum v1.44). Bean fix: replaced `@ComponentScan` with `@Import` of 6 production classes + `@EntityScan` to avoid `BeanDefinitionOverrideException` from test classpath contamination.

- [x] **Session 4.7:** Phase 4 Completion — Dashboard-API wiring (`arbitrage.simulation.*` block added to `dashboard-api/application.yml`, 20 env-var-overrideable properties), ADR-013 (price replay vs Monte Carlo), `docs/phase-reports/PHASE-4.md` (16 component table, use cases, 7 design decisions, interview Q&A).

**567 tests total** (34 common-models + 251 exchange-connectors + 139 normalisation-engine + 92 detection-engine + 51 execution-simulator). All passing.

**Phase 4 complete.**

### Phase 5 — Real-Time Dashboard (in progress)

- [x] **Session 5.1:** dashboard-ui bootstrap — Vite 8 + React 18 + TypeScript 5 + TailwindCSS 3. Dependencies: TanStack Query v5, Zustand v4, Recharts, React Router v6. Files: `src/types/index.ts` (domain types mirroring Java models), `src/utils/logger.ts` (dev-only logger), `src/stores/arbitrageStore.ts` (Zustand: selectedPair, wsStatus), `src/main.tsx` (QueryClientProvider), `src/App.tsx` (BrowserRouter + 4 NavLinks + WS status dot). Vite proxy: `/api` → `:8080`, `/ws` → `ws://:8080`. Build: 79 modules, 0 errors, 550ms.

- [x] **Session 5.2:** Live price feed — Backend: `TickWebSocketHandler` (Sinks.Many multicast, `/ws/ticks` endpoint), `WebSocketConfig` (SimpleUrlHandlerMapping.setUrlMap, WebSocketHandlerAdapter), `TickBroadcaster` (KafkaListener on normalised-ticks, groupId=dashboard-ws-ticks-group, forwards raw JSON). Frontend: `useWebSocket<T>` (exponential-backoff reconnect 1s→30s), `useLivePrices` (per-exchange LivePriceMap, 500ms stale threshold), `PriceTable` (3 rows, 200ms re-render, stale opacity/dot), `LiveMonitorPage` (pair selector + live table). Build: 84 modules, 0 errors, 531ms.

- [x] **Session 5.3:** Opportunity feed — Backend: `OpportunityWebSocketHandler` (same Sinks.Many multicast, `/ws/opportunities`), `WebSocketConfig` updated (both `/ws/ticks` + `/ws/opportunities` in same mapping), `OpportunityBroadcaster` (KafkaListener on arbitrage-opportunities, groupId=dashboard-ws-opportunities-group). Frontend: `useOpportunities` hook (deduplicates by id, ring buffer 50, in-place update on lifecycle events), `OpportunityRow` component (status badge DETECTED/OPEN/CLOSED/EXPIRED, exchange direction, net spread bps, profit, duration), full `OpportunitiesPage`. Build: 86 modules, 0 errors, 558ms.

- [x] **Session 5.4:** Analytics REST API + charts — Backend: `AnalyticsRepository` (JdbcTemplate native SQL on `arbitrage_opportunities`+`simulation_results`, 3 query methods), `AnalyticsController` (3 REST endpoints, `Mono.fromCallable().subscribeOn(Schedulers.boundedElastic())` for blocking JDBC in WebFlux), 3 DTO records. Frontend: `useAnalytics.ts` (3 TanStack Query hooks, 30s poll / 25s staleTime), full `AnalyticsPage` (summary stat cards + hourly BarChart + exchange route horizontal BarChart, Recharts). Build: 0 errors (646 modules — Recharts bundled in).

- [x] **Session 5.5:** HealthPage — Backend: `getLastSeenAgeMs(ExchangeId)` added to `FeedHealthMonitor`, `ExchangeHealthDto`, `HealthController` (`GET /api/health/exchanges`, per-exchange CONNECTED/STALE/DISCONNECTED + last-tick age). Frontend: `/actuator` Vite proxy, `useHealthData.ts` (useExchangeHealth 5s/useSystemHealth 10s), full `HealthPage` (exchange feed cards with status badge + skeleton loading, infrastructure cards from Actuator). Build: 647 modules, 0 errors.

- [x] **Session 5.6:** Historical Opportunity Table + Detail Modal + URL Filters — Pre-session audit fixed 4 bugs (Java assert → Objects.requireNonNull, missing 100ms UI throttle, missing Swagger/SpringDoc, skeleton WEBSOCKET_API.md). Backend: `ClosedOpportunityDto`, `SimulationSummaryDto`, `OpportunityPageDto`, `OpportunityRepository` (JdbcTemplate, dynamic filter SQL), `OpportunityController` (3 REST endpoints: paginated list, detail, simulation). Frontend: `useHistoricalOpportunities.ts` (3 TanStack Query hooks; detail+simulation enabled only when modal open), `OpportunityDetailModal.tsx` (fee breakdown, timeline, simulation panel), `OpportunitiesPage.tsx` rewritten (URL-synced filters via `useSearchParams`, historical CLOSED table with pagination, modal wiring). ADR-014: hybrid WebSocket + REST polling architecture. Build: ~657 modules.

- [x] **Session 5.7:** Live Monitor — CrossExchangeSpreadMatrix + OpportunityCounter — `CrossExchangeSpreadMatrix.tsx`: 3×3 net spread matrix for all 6 exchange directions, computed client-side from live WebSocket prices (no new backend endpoint). Fee constant `TAKER_FEE_RATE=0.001` matches backend config; stale prices (>1s) dimmed. `OpportunityCounter.tsx`: 1h and 24h closed opportunity counts using existing `useAnalyticsSummary(1)` and `useAnalyticsSummary(24)` with skeleton loading. `LiveMonitorPage.tsx` updated with both components in a clean layout. No new backend endpoints, no new dependencies.

- [x] **Session 5.8:** Analytics Charts 2–4 — `SpreadBucketDto`, `DurationBucketDto`, `CumulativePnlPointDto` records. `AnalyticsRepository` + `AnalyticsController` extended with 3 new SQL queries and 3 new REST endpoints: `GET /api/analytics/spread-distribution` (CASE bucketing on `net_spread_bps`, 6 buckets), `GET /api/analytics/duration-distribution` (CASE bucketing on `total_duration_ms`, CLOSED/EXPIRED only), `GET /api/analytics/cumulative-pnl` (CTE deduplication + `SUM() OVER` window function). Frontend: 3 new TanStack Query hooks, 3 new Recharts chart sections in `AnalyticsPage.tsx` — Spread Distribution BarChart (red=Negative, green=positive via Cell), Duration Distribution BarChart (blue), Cumulative P&L LineChart (two lines: theoretical=green, simulated=blue).

- [x] **Session 5.9:** Analytics Charts 5–7 — `LatencyProfitabilityPointDto`, `HeatmapCellDto`, `FalsePositiveRatePointDto` records. 3 new SQL queries + 3 new REST endpoints: `GET /api/analytics/latency-profitability`, `GET /api/analytics/heatmap` (EXTRACT(DOW/HOUR) bucketing, 7-day window), `GET /api/analytics/false-positive-rate` (CTE dedup + FILTER conditional aggregation). Frontend: OLS linear regression trendline computed client-side (`computeTrendLine`, `computeBreakevenLatencyMs`). Recharts `ComposedChart` (Scatter + Line trendline + ReferenceLine y=0). CSS Grid heatmap (no new dependency). False Positive Rate LineChart with 50% reference line.

- [x] **Session 5.10:** Health Page — JVM metrics (heap progress bar green/amber/red + live threads, from Spring Actuator `jvm.memory.*` + `jvm.threads.live`) and Kafka consumer lag gauge (from `kafka.consumer.records-lag-max`, green/amber/red at <10/10-100/>100, graceful unavailable state). Vitest installed + configured (`vitest/config` import, `test: vitest run` script). `computeTrendLine` + `computeBreakevenLatencyMs` extracted to `src/utils/analytics.ts`. 15 unit tests (`src/utils/analytics.test.ts`) — all passing. Phase 5 Completion Report: `docs/phase-reports/PHASE-5.md`.

**Phase 5 complete. Sessions 5.1–5.10 complete. 15 frontend unit tests. 557 backend tests.**

- [x] **Post-Phase-5 Demo Mode Fix:** Three-part fix for empty dashboard. (1) `dashboard-api/application.yml` missing `exchanges:` section — `FeeConfiguration` was silently using Java defaults (20 bps fees). Added section with `${EXCHANGE_*_TAKER_FEE}` env var binding. (2) Near-zero demo fees: `EXCHANGE_*_TAKER_FEE=0.000001` (0.02 bps round trip) so real BTC/USDT cross-exchange spreads (0.1–0.5 bps) fire as opportunities. (3) `OpportunityPersistenceListener` — the missing Kafka → TimescaleDB writer for `arbitrage_opportunities`. Previous pipeline: detection → Kafka → WebSocket (no DB write). Now: detection → Kafka → WebSocket + DB (upsert with `ON CONFLICT (id, detection_timestamp)`). Result: REST API returns live data (`totalCount=157+`, `avgNetSpreadBps=0.52`).

- [x] **Phase 6 Pre-Session (2026-06-09):** Two improvements. (1) WS status indicator fix: `WebSocketProvider` lifts both `/ws/ticks` + `/ws/opportunities` connections to App level — status dot now correctly reflects CONNECTED on all pages (Analytics, Opportunities, Health), not just Live Monitor. Global status derived from both feeds using CONNECTED > CONNECTING > ERROR > DISCONNECTED priority. (2) Multi-pair: ETH-USDT + BNB-USDT added to all 3 exchange connectors (dynamic multi-symbol subscription strings) and all 3 parsers (symbol→pair lookup maps derived from `TradingPairsProperties`). Two-constructor pattern preserves backward compatibility for all 567 backend tests. `application.yml` updated with 3 pairs.

- [x] **Session 6.1 (2026-06-09):** LatencyContext Pipeline Instrumentation. `KafkaHeaderUtils` in common-models (T0-T9 constants, 8-byte big-endian encode/decode). All 3 connector producers stamp T0/T1/T2 via `ProducerRecord` headers. `NormalisationService` reads T0-T2 from inbound headers, stamps T3/T4/T5 on outbound (3-method structure: ConsumerRecord listener → test-facing 2-arg overload → private core). `ArbitrageDetectionEngine` reads T0-T5, captures T6 (consume), T7 (Redis), T8 (comparison) via array trick in reactive chain, builds `LatencyContext`. `OpportunityKafkaPublisher` stamps T9 and writes all T0-T9 to `arbitrage-opportunities` headers. `OpportunityPersistenceListener` reads T0-T9, logs structured pipeline breakdown per DETECTED event. 578 backend tests (6 new KafkaHeaderUtilsTest, 4 updated test files for ProducerRecord API).

- [x] **Session 6.2 (2026-06-09):** HdrHistogram Integration. `LatencyRecorder` (6 `org.HdrHistogram.Recorder` instances, one per stage, two-histogram flip pattern). `LatencyMetricsPublisher` (`@PostConstruct` registers 24 Micrometer gauges, `@Scheduled` 10s snapshot loop, `getLatestPercentiles()` for REST). `LatencyController` (`GET /api/latency/percentiles` → `LatencyPercentilesDto`). `OpportunityKafkaPublisher.publish()` returns `long t9`. `ArbitrageDetectionEngine.publishAndRecord()` builds complete `LatencyContext` with T9 before calling `latencyRecorder.record()`. First measured p50 values: normaliser-processing=63µs, detection=3.8ms, end-to-end=391ms.

- [x] **Session 6.3 (2026-06-09):** TickReplayTool for Load Testing. `ReplayKafkaProducerConfig` (`replayNormalisedTickKafkaTemplate` bean, distinct name to avoid collision with `normalisedTickKafkaTemplate`). `ReplayMode` enum (REALTIME=90/sec, FAST_5X=450/sec, FAST_10X=900/sec, BURST=1000msgs/100ms/5s pause). `ReplayResult` record with before/after HdrHistogram snapshots and `performanceCliffDetected` flag (END_TO_END p99 crossing 100ms). `TickReplayTool` with `LockSupport.parkNanos` rate control, `AtomicBoolean` concurrency guard, Gaussian-noise synthetic tick generation, 12s post-run settle for HdrHistogram snapshot. `LoadTestController` (`POST /api/load-test/run`, `GET /api/load-test/status`). 7 new TickReplayToolTest tests. 550 backend tests.

- [x] **Session 6.4 (2026-06-10):** Bottleneck Identification + Optimization. Three changes on the hot path: (1) Redis: `PriceStateService` switched from Hash (HSET+EXPIRE+3×HGETALL = 5 cmds) to String (SET+PX+MGET = 2 cmds) — `PriceState` gained `@Jacksonized`, `PriceStateService` injects `ObjectMapper`, `PriceStateServiceTest` completely rewritten from `ReactiveHashOperations` to `ReactiveValueOperations` mocks with real JSON round-trip tests. (2) Kafka consumer `fetch.max.wait.ms`: 500ms → 10ms in `DetectionKafkaConsumerConfig`. (3) Kafka producer `linger.ms`: 5 → 2 in `NormalisationKafkaProducerConfig`. **Results: DETECTION_PROCESSING p50 3.2ms→2.24ms (-30%), p99 7ms→4.31ms (-38%).** 550 backend tests (13 PriceStateService tests rewritten). Server started clean; DEBUG logs confirm SET+PX write + MGET read live.

- [x] **Session 6.5 (2026-06-12):** JVM Tuning Experiments. GC monitoring infrastructure: `GcBeanStats` + `GcSnapshot` records (JVM GC state capture), `GcStatsService` (reads live `GarbageCollectorMXBean` via `ManagementFactory`), `JvmExperimentResult` (before/after GC delta + latency percentiles), `JvmTuningController` (`GET /api/jvm-tuning/gc-stats`, `POST /api/jvm-tuning/run-experiment`). `JVM_GC_ARGS` env var injection in `dashboard-api/build.gradle.kts` bootRun task. `run-jvm-experiments.ps1` automation script with 6 configs (G1GC_512MB/2GB/PRETOUCH, ZGC_512MB/2GB/PRETOUCH) and Kafka backlog warmup gate. Measured G1GC_512MB baseline: DETECTION_PROCESSING p50=2.71ms p99=6.46ms, 35 GC events / 90ms GC time in 30s test. ZGC startup STW: **4ms** vs G1GC STW: 222ms (55× lower). Key design limitation found and documented: `LatencyRecorder.record()` only fires on detected opportunities (T9≠0); single-source synthetic ticks don't generate cross-exchange spreads → histogram unpopulated by load test. 7 new `GcStatsServiceTest` tests. 557 backend tests.

- [x] **Session 6.6 (2026-06-14/15):** All 6 JVM Configs Run + Performance Report Finalized. Ran all configs via `run-jvm-experiments.ps1` (60s each, consistent dataset). Fixed 4 automation bugs: Gradle daemon env-var caching (switched to `java -jar` fat JAR), JAR path-with-spaces in `Start-Process -ArgumentList` array, `$resultsDir` not created before loop, `/actuator/health` returning 503 during Kafka rebalance → switched to TCP port binding check. Fixed 2 frontend bugs: negative age display (`Math.max(0, ...)`) and WebSocket sessions force-closing on backpressure (`Mono.firstWithSignal(send, receive.then())`). **Winner: G1GC_2GB** (DETECTION p99=11.87ms, E2E p99=159ms). ZGC underperformed on dev machine (CPU contention with Docker). `docs/performance/PERFORMANCE_REPORT.md` complete. 557 backend tests + 15 frontend tests.

**Phase 6 COMPLETE.**

---

## Performance Targets

| Metric | Target |
|--------|--------|
| End-to-end latency (tick → detection) | < 100ms p99 |
| Tick ingestion throughput | 1000+ msgs/sec per exchange |
| Detection comparison | < 1ms per pair per tick |
| Dashboard update rate | 100ms throttle (10 FPS) |
| Staleness threshold | 500ms (configurable) |

Performance report with actual measurements will be published in Phase 6.

---

## License

Private project. Not open-source.
