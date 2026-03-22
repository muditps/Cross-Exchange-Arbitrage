rootProject.name = "arbitrage-detector"

// ============================================================
// Multi-Asset Cross-Exchange Arbitrage Detection Platform
// Module declarations — each maps to a service boundary
// ============================================================

include("common-models")           // Shared domain objects (NormalisedTick, ArbitrageOpportunity, etc.)
include("exchange-connectors")     // WebSocket clients for Binance, Bybit, KuCoin (+ NSE/BSE in Phase 7)
include("normalisation-engine")    // Raw ticks → unified NormalisedTick schema
include("detection-engine")        // Redis state + cross-exchange comparison + opportunity lifecycle
include("execution-simulator")     // Simulated execution: latency, slippage, fill probability, net P&L
include("dashboard-api")           // Spring Boot WebSocket server + REST for historical queries
