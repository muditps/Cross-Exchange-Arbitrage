# ============================================================
# Multi-stage Dockerfile for the Arbitrage Detection Platform
# ============================================================
# Stage 1: Build the application with Gradle
# Stage 2: Run the application with a minimal JRE
#
# KEY CONCEPT — Multi-Stage Builds:
# Stage 1 uses a full JDK (large image, ~500MB) to compile.
# Stage 2 copies only the built JAR into a minimal JRE image (~200MB).
# The final image doesn't contain Gradle, source code, or build tools.
# This reduces image size, attack surface, and startup time.
#
# KEY CONCEPT — Why Eclipse Temurin:
# Temurin is the most widely adopted free OpenJDK distribution,
# maintained by the Adoptium project. It's what most trading firms
# and enterprises use in production.
# ============================================================

# --- Stage 1: Build ---
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app

# Copy Gradle wrapper and build files first (for Docker layer caching).
# These change rarely, so Docker caches this layer. When only source
# code changes, Docker skips re-downloading dependencies.
COPY gradlew ./
COPY gradle/ gradle/
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle/libs.versions.toml gradle/

# Copy all module build files
COPY common-models/build.gradle.kts common-models/
COPY exchange-connectors/build.gradle.kts exchange-connectors/
COPY normalisation-engine/build.gradle.kts normalisation-engine/
COPY detection-engine/build.gradle.kts detection-engine/
COPY execution-simulator/build.gradle.kts execution-simulator/
COPY dashboard-api/build.gradle.kts dashboard-api/

# Download dependencies (cached layer — only re-runs when build files change)
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon || true

# Copy source code (changes frequently — this layer is rebuilt often)
COPY . .

# Build the bootable JAR (skip tests — CI already ran them)
RUN ./gradlew :dashboard-api:bootJar --no-daemon -x test

# --- Stage 2: Run ---
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/dashboard-api/build/libs/*.jar app.jar

# Expose the application port
EXPOSE 8080

# Health check: verify the application responds
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

# Run with ZGC (low-pause garbage collector) for latency-sensitive workloads.
# -XX:+UseZGC: Z Garbage Collector — sub-millisecond GC pauses regardless of heap size.
# -XX:+ZGenerational: Generational mode (Java 21+) — better throughput than non-generational ZGC.
# -Xmx512m: Max heap 512MB — sufficient for our workload, prevents unbounded memory growth.
ENTRYPOINT ["java", \
    "-XX:+UseZGC", \
    "-XX:+ZGenerational", \
    "-Xmx512m", \
    "-jar", "app.jar"]
