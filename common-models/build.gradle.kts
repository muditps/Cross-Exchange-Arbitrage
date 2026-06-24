// ============================================================
// common-models — Shared Domain Objects
// ============================================================
// Contains all domain classes shared across the pipeline:
// NormalisedTick, ArbitrageOpportunity, ExchangeId, TradingPair,
// LatencyContext, fee configuration, and status enums.
//
// This module has NO Spring Boot dependency — it is a pure Java library.
// Every other module depends on this one.
// ============================================================

plugins {
    `java-library`
}

dependencies {
    // Kafka header types (Header, Headers) — compileOnly because all consuming modules
    // already have kafka-clients on their classpath via spring-kafka
    compileOnly(libs.kafka.clients)
    testImplementation(libs.kafka.clients)

    // Jackson annotations for JSON serialization
    api(libs.jackson.databind)
    api(libs.jackson.datatype.jsr310)

    // Jakarta Validation for input constraints
    api("jakarta.validation:jakarta.validation-api:3.1.0")

    // Lombok — repeated here (also in root subprojects{}) because VS Code's
    // Java Language Server (Eclipse JDT) does not always resolve compileOnly
    // dependencies declared in the root build file for individual modules.
    // Gradle deduplicates automatically, so this has zero effect on the build.
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
}
