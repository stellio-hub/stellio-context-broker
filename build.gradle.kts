import com.google.cloud.tools.jib.gradle.PlatformParameters
import io.gitlab.arturbosch.detekt.Detekt
import io.gitlab.arturbosch.detekt.DetektCreateBaselineTask
import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

buildscript {
    dependencies {
        classpath("com.google.cloud.tools:jib-layer-filter-extension-gradle:0.3.0")
    }
}

extra["springCloudVersion"] = "2025.1.1"

// Kotlin artifacts that must be pinned to 2.4.0 in the compiler-facing configurations.
// kotlin-dsl (Gradle's embedded plugin) brings these at 2.3.20 onto the resolution path.
val kgpVersion = "2.4.0"
val kotlinCompilerArtifacts = setOf(
    "kotlin-compiler-embeddable", "kotlin-daemon-client", "kotlin-daemon-embeddable",
    "kotlin-compiler-runner", "kotlin-build-tools-impl", "kotlin-build-tools-api"
)

plugins {
    // https://docs.spring.io/spring-boot/docs/current/gradle-plugin/reference/htmlsingle/#reacting-to-other-plugins.java
    java
    `kotlin-dsl`
    // only apply the plugin in the subprojects requiring it because it expects a Spring Boot app
    // and the shared lib is obviously not one
    id("org.springframework.boot") version "4.1.0-RC1" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
    kotlin("jvm") version "2.4.0" apply false
    kotlin("plugin.spring") version "2.4.0" apply false
    id("com.google.cloud.tools.jib") version "3.5.3" apply false
    id("io.gitlab.arturbosch.detekt") version "1.23.8" apply false
    id("org.sonarqube") version "7.3.1.8318"
    jacoco
}

subprojects {
    repositories {
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }

    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.spring")
    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "io.gitlab.arturbosch.detekt")
    apply(plugin = "jacoco")

    java.sourceCompatibility = JavaVersion.VERSION_25

    the<DependencyManagementExtension>().apply {
        imports {
            mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
        }
    }

    dependencies {
        implementation("org.jetbrains.kotlin:kotlin-reflect")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
        implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")

        implementation("org.springframework.boot:spring-boot-starter-actuator")
        implementation("org.springframework.boot:spring-boot-starter-webflux")
        implementation("org.springframework.boot:spring-boot-starter-security-oauth2-resource-server")
        implementation("org.springframework.boot:spring-boot-starter-validation")

        implementation("org.springframework.boot:spring-boot-starter-opentelemetry")
        implementation("io.opentelemetry.instrumentation:opentelemetry-logback-appender-1.0:2.25.0-alpha")

        implementation("org.springframework.boot:spring-boot-starter-kafka")

        implementation("tools.jackson.module:jackson-module-kotlin")

        implementation("com.apicatalog:titanium-json-ld:1.7.0")
        implementation("org.glassfish:jakarta.json:2.0.1")

        implementation("com.github.stellio-hub:json-merge:0.1.0")
        implementation("org.json:json:20260522")

        implementation("io.arrow-kt:arrow-fx-coroutines:2.2.3")

        implementation("org.locationtech.jts.io:jts-io-common:1.20.0")

        annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

        runtimeOnly("io.micrometer:micrometer-registry-otlp")

        testImplementation("org.springframework.boot:spring-boot-starter-webflux-test")
        testImplementation("org.springframework.boot:spring-boot-starter-webclient-test")
        testImplementation("org.springframework.boot:spring-boot-testcontainers")
        testImplementation("org.springframework.boot:spring-boot-starter-security-test")
        testImplementation("com.ninja-squad:springmockk:5.0.1")
        testImplementation("io.mockk:mockk:1.14.11")
        testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
        testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    }

    kotlin {
        compilerOptions {
            // -Xconsistent-data-class-copy-visibility was stabilised (became default) in Kotlin 2.1; removed here.
            // -Xannotation-default-target=param-property became default in Kotlin 2.4; removed here.
            freeCompilerArgs.addAll("-Xjsr305=strict")
            languageVersion.set(KotlinVersion.KOTLIN_2_4)
            apiVersion.set(KotlinVersion.KOTLIN_2_4)
            jvmTarget.set(JvmTarget.JVM_25)
        }
        jvmToolchain(25)
    }
    tasks.withType<Test> {
        environment("SPRING_PROFILES_ACTIVE", "test")
        // Spring Cloud 2025.1.1 only declares compatibility with Boot 4.0.x; disable the verifier
        // on this experimental branch until a Spring Cloud BOM for Boot 4.1.x is released.
        systemProperty("spring.cloud.compatibility-verifier.enabled", "false")
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
        jvmArgs("-Dmockk.junit.extension.checkUnnecessaryStub=true")
    }

    // The kotlin-dsl plugin (applied at root level) brings Gradle's embedded Kotlin artifacts (2.3.20) onto
    // the resolution path for kotlinCompilerClasspath. KGP 2.4.0's build-tools-impl reads
    // KotlinCompilerVersion.VERSION from kotlin-compiler-embeddable; if it resolves 2.3.20, it wraps itself
    // in KotlinWrapperPre2_4_0 which has an NPE with plugin classpaths (KT-76485 class of bug).
    // Force the compiler-facing configurations to the project's KGP version.
    configurations.matching { it.name.startsWith("kotlinCompilerClasspath") || it.name == "kotlinBuildToolsApiClasspath" }.all {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.jetbrains.kotlin" && requested.name in kotlinCompilerArtifacts) {
                useVersion(kgpVersion)
            }
        }
    }

    // see https://github.com/detekt/detekt/issues/6198
    // detekt 1.23.x ships with Kotlin 1.9 internals; pin its kotlin dependencies to the version it declares
    // so that Gradle's transform infrastructure does not pick up the project's Kotlin 2.4.0 artifacts.
    configurations.matching { it.name == "detekt" }.all {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.jetbrains.kotlin") {
                useVersion(io.gitlab.arturbosch.detekt.getSupportedKotlinVersion())
            }
        }
    }

    tasks.withType<Detekt>().configureEach {
        config.setFrom(files("$rootDir/config/detekt/detekt.yml"))
        buildUponDefaultConfig = true
        baseline.set(file("$projectDir/config/detekt/baseline.xml"))
        source("src/main/kotlin", "src/test/kotlin", "src/testFixtures/kotlin")
        // detekt 1.23.x ships Kotlin 1.9 internals which only support JVM targets up to 22;
        // override here so detekt does not inherit the project-wide JVM 25 target.
        jvmTarget = "22"

        reports {
            xml.required.set(true)
            txt.required.set(false)
            html.required.set(true)
        }
    }
    tasks.withType<DetektCreateBaselineTask>().configureEach {
        config.setFrom(files("$rootDir/config/detekt/detekt.yml"))
        buildUponDefaultConfig.set(true)
        baseline.set(file("$projectDir/config/detekt/baseline.xml"))
    }

    // see https://docs.gradle.org/current/userguide/jacoco_plugin.html for configuration instructions
    // 0.8.12+ is required for Java 25 support
    jacoco {
        toolVersion = "0.8.13"
    }
    tasks.test {
        finalizedBy(tasks.jacocoTestReport) // report is always generated after tests run
    }
    tasks.withType<JacocoReport> {
        dependsOn(tasks.test) // tests are required to run before generating the report
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    tasks.withType<com.google.cloud.tools.jib.gradle.BuildImageTask>().configureEach {
        notCompatibleWithConfigurationCache("Jib does not support configuration cache")
    }

    project.ext.set("jibFromImage", "eclipse-temurin:25-jre")
    project.ext.set(
        "jibFromPlatforms",
        listOf(
            PlatformParameters().apply {
                os = "linux"
                architecture = "arm64"
            },
            PlatformParameters().apply {
                os = "linux"
                architecture = "amd64"
            }
        )
    )
    project.ext.set("jibContainerCreationTime", "USE_CURRENT_TIMESTAMP")
    project.ext.set(
        "jibContainerLabels",
        mapOf(
            "maintainer" to "EGM",
            "org.opencontainers.image.authors" to "EGM",
            "org.opencontainers.image.documentation" to "https://stellio.readthedocs.io/",
            "org.opencontainers.image.vendor" to "EGM",
            "org.opencontainers.image.licenses" to "Apache-2.0",
            "org.opencontainers.image.title" to "Stellio context broker",
            "org.opencontainers.image.description" to
                """
                    Stellio is an NGSI-LD compliant context broker developed by EGM. 
                    NGSI-LD is an Open API and data model specification for context management published by ETSI.
                """.trimIndent(),
            "org.opencontainers.image.source" to "https://github.com/stellio-hub/stellio-context-broker",
            "com.java.version" to "${JavaVersion.VERSION_25}"
        )
    )
}

allprojects {
    group = "com.egm.stellio"
    version = "latest-dev"

    repositories {
        mavenCentral()
        maven { url = uri("https://repo.spring.io/milestone") }
    }

    sonarqube {
        properties {
            property("sonar.projectKey", "stellio-hub_stellio-context-broker")
            property("sonar.organization", "stellio-hub")
            property("sonar.host.url", "https://sonarcloud.io")
        }
    }
}
