// ============================================================
// detection-engine — Arbitrage Detection
// ============================================================
// Maintains price state in Redis, compares across exchanges,
// detects arbitrage opportunities, and manages opportunity lifecycle.
// This module contains the most correctness-critical code:
// all financial calculations use BigDecimal.
// ============================================================

plugins {
    id("java-library")
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":common-models"))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation(libs.spring.boot.starter.data.redis.reactive)
    implementation(libs.spring.kafka)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.hdr.histogram)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.spring.kafka.test)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation("io.projectreactor:reactor-test") // StepVerifier for reactive assertions
    testImplementation("org.springframework.boot:spring-boot-starter-validation") // Hibernate Validator for @Validated on FeeConfiguration
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}

tasks.named<Jar>("jar") {
    enabled = true
}
