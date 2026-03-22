// ============================================================
// dashboard-api — Backend API + WebSocket Server
// ============================================================
// Spring Boot application serving as the backend for the dashboard.
// Provides: WebSocket endpoints for real-time price/opportunity/health push,
// REST endpoints for historical queries, Swagger API docs,
// and the Prometheus metrics endpoint.
// This is the ONLY module that produces a bootable JAR (bootJar enabled).
// ============================================================

plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":common-models"))
    implementation(project(":exchange-connectors"))
    implementation(project(":normalisation-engine"))
    implementation(project(":detection-engine"))
    implementation(project(":execution-simulator"))

    implementation(libs.spring.boot.starter.webflux)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.spring.boot.starter.data.redis.reactive)
    implementation(libs.spring.boot.starter.data.jpa)
    implementation(libs.spring.boot.starter.validation)
    implementation(libs.spring.kafka)
    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.flyway.core)
    implementation(libs.flyway.database.postgresql)
    runtimeOnly(libs.postgresql)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.spring.kafka.test)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgresql)
}
