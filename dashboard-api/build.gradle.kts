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
    implementation(libs.springdoc.openapi.starter.webflux.ui)
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

// Pass JVM_GC_ARGS env var as JVM flags to bootRun for GC tuning experiments.
// Example: JVM_GC_ARGS="-XX:+UseZGC -Xmx2g -Xms1g" ./gradlew :dashboard-api:bootRun
tasks.named<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    // Load .env file so bootRun picks up TIMESCALEDB_PASSWORD and other secrets
    // without requiring them to be set in the system environment first.
    val envFile = rootProject.file(".env")
    if (envFile.exists()) {
        envFile.readLines().forEach { line ->
            val trimmed = line.trim()
            if (trimmed.isNotEmpty() && !trimmed.startsWith("#")) {
                val eqIdx = trimmed.indexOf('=')
                if (eqIdx > 0) {
                    val key = trimmed.substring(0, eqIdx).trim()
                    val value = trimmed.substring(eqIdx + 1).trim()
                    environment(key, value)
                }
            }
        }
    }

    val gcArgs = System.getenv("JVM_GC_ARGS")
    if (!gcArgs.isNullOrBlank()) {
        jvmArgs(gcArgs.trim().split("\\s+".toRegex()))
        println("[JVM Tuning] JVM_GC_ARGS applied: $gcArgs")
    }
}
