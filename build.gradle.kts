// ============================================================
// Multi-Asset Cross-Exchange Arbitrage Detection Platform
// Root Build Configuration
// ============================================================
// This root build applies shared configuration to ALL submodules:
// Java 21 toolchain, common dependencies, test setup, and code style.
// Module-specific dependencies are declared in each module's build.gradle.kts.
// ============================================================

plugins {
    java
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
    group = "com.arbitrage"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    // ---- Java 21 Toolchain ----
    // Enforced at root level so all modules use the same JDK version.
    // Prevents "it compiles on my machine" issues across different environments.
    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    // ---- Shared Dependencies ----
    // Access version catalog from subproject context via the catalog API.
    // Dependencies every module needs: Lombok for boilerplate reduction,
    // JUnit 5 for testing. Module-specific deps go in each module's build file.
    val catalogLibs = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")

    dependencies {
        // Lombok — constructor injection, builders, getters, @Slf4j logging
        compileOnly(catalogLibs.findLibrary("lombok").get())
        annotationProcessor(catalogLibs.findLibrary("lombok").get())
        testCompileOnly(catalogLibs.findLibrary("lombok").get())
        testAnnotationProcessor(catalogLibs.findLibrary("lombok").get())

        // Testing — JUnit 5 + Mockito
        testImplementation(catalogLibs.findLibrary("junit-jupiter").get())
        testImplementation(catalogLibs.findLibrary("mockito-core").get())
        testImplementation(catalogLibs.findLibrary("mockito-junit-jupiter").get())
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    // ---- Test Configuration ----
    tasks.withType<Test> {
        useJUnitPlatform()
        // Show test results in console for quick feedback
        testLogging {
            events("passed", "skipped", "failed")
            showStandardStreams = false
            showExceptions = true
            showCauses = true
        }
    }

    // ---- Compiler Settings ----
    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        // Enable all recommended warnings — treat warnings seriously
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-processing"))
    }
}
