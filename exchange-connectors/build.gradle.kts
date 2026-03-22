// ============================================================
// exchange-connectors — WebSocket Clients
// ============================================================
// WebSocket clients for each exchange (Binance, Bybit, KuCoin).
// Each connector implements the ExchangeConnector interface (Strategy Pattern).
// Handles connection lifecycle, reconnection, heartbeats, and raw message parsing.
// Publishes raw ticks to exchange-specific Kafka topics.
// ============================================================

plugins {
    id("java-library")
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":common-models"))

    implementation(libs.spring.boot.starter.webflux)
    implementation(libs.spring.kafka)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.micrometer.registry.prometheus)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.wiremock)
}

// Disable bootJar — this is a library module, not a standalone application
tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}

tasks.named<Jar>("jar") {
    enabled = true
}
