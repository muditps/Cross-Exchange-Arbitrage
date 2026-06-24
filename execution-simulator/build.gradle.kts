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
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation(libs.spring.boot.starter.data.jpa)
    implementation(libs.spring.kafka)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.flyway.core)
    implementation(libs.flyway.database.postgresql)
    runtimeOnly(libs.postgresql)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.spring.kafka.test)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgresql)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
}

// docker-java 3.4.1 (bundled with Testcontainers 1.20.x) defaults to Docker API v1.41.
// Docker Desktop 4.64+ requires minimum API v1.44 — versioned requests below v1.44 get HTTP 400.
// system properties are used because environment() doesn't reliably propagate to forked test JVMs.
tasks.withType<Test> {
    systemProperty("docker.host", System.getenv("DOCKER_HOST") ?: "tcp://localhost:2375")
    systemProperty("api.version", "1.44")
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}

tasks.named<Jar>("jar") {
    enabled = true
}
