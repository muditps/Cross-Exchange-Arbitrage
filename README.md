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
