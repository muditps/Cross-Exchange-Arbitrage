// ============================================================
// execution-simulator — Simulated Trade Execution
// ============================================================
// Models whether a detected opportunity could have been captured:
// simulates network latency, exchange processing time,
// price movement during execution delay, and calculates net profit
// after fees and slippage. Results persisted to TimescaleDB.
// ============================================================

plugins {
    id("java-library")
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":common-models"))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation(libs.spring.boot.starter.data.jpa)
    implementation(libs.spring.kafka)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.flyway.core)
    implementation(libs.flyway.database.postgresql)
    runtimeOnly(libs.postgresql)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgresql)
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}

tasks.named<Jar>("jar") {
    enabled = true
}
