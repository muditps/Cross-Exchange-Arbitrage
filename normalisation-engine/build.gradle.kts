// ============================================================
// normalisation-engine — Feed Normalisation Pipeline
// ============================================================
// Consumes raw ticks from exchange-specific Kafka topics,
// transforms them into the unified NormalisedTick schema,
// and publishes to the normalised-ticks topic.
// One transformer per exchange, selected by TickTransformerFactory.
// ============================================================

plugins {
    id("java-library")
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":common-models"))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation(libs.spring.kafka)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.micrometer.registry.prometheus)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.kafka)
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = false
}

tasks.named<Jar>("jar") {
    enabled = true
}
