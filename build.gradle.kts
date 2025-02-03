import com.google.cloud.tools.jib.gradle.PlatformParameters
import io.gitlab.arturbosch.detekt.Detekt
import io.gitlab.arturbosch.detekt.DetektCreateBaselineTask
import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

buildscript {
    dependencies {
        classpath("com.google.cloud.tools:jib-spring-boot-extension-gradle:0.1.0")
    }
}

extra["springCloudVersion"] = "2024.0.0-RC1"

plugins {
    // https://docs.spring.io/spring-boot/docs/current/gradle-plugin/reference/htmlsingle/#reacting-to-other-plugins.java
    java
    `kotlin-dsl`
    // only apply the plugin in the subprojects requiring it because it expects a Spring Boot app
    // and the shared lib is obviously not one
    id("org.springframework.boot") version "3.4.2" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
    id("org.graalvm.buildtools.native") version "0.10.4"
    kotlin("jvm") version "2.1.10" apply false
    kotlin("plugin.spring") version "2.1.10" apply false
    id("com.google.cloud.tools.jib") version "3.4.4" apply false
    id("io.gitlab.arturbosch.detekt") version "1.23.7" apply false
    id("org.sonarqube") version "6.0.1.5171"
    jacoco
}

subprojects {
    repositories {
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }

    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.spring")
    apply(plugin = "io.gitlab.arturbosch.detekt")
    apply(plugin = "jacoco")

    java.sourceCompatibility = JavaVersion.VERSION_21

    the<DependencyManagementExtension>().apply {
        imports {
            mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
        }
    }

    dependencies {
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
        implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")

        implementation("org.springframework.boot:spring-boot-starter-actuator")
        implementation("org.springframework.boot:spring-boot-starter-webflux")
        implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
        implementation("org.springframework.boot:spring-boot-starter-security")
        implementation("org.springframework.boot:spring-boot-starter-validation")
        // it provides support for JWT decoding and verification
        implementation("org.springframework.security:spring-security-oauth2-jose")

        implementation("org.springframework.kafka:spring-kafka")

        implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

        implementation("com.apicatalog:titanium-json-ld:1.4.1")
        implementation("org.glassfish:jakarta.json:2.0.1")

        implementation("io.arrow-kt:arrow-fx-coroutines:2.0.1")

        implementation("org.locationtech.jts.io:jts-io-common:1.20.0")

        annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

        runtimeOnly("de.siegmar:logback-gelf:6.1.1")
        runtimeOnly("io.micrometer:micrometer-registry-prometheus")

        testImplementation("org.springframework.boot:spring-boot-starter-test")
        testImplementation("org.springframework.boot:spring-boot-testcontainers")
        testImplementation("io.projectreactor:reactor-test")
        testImplementation("com.ninja-squad:springmockk:4.0.2")
        testImplementation("org.springframework.security:spring-security-test")
        testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    }

    kotlin {
        compilerOptions {
            // https://kotlinlang.org/docs/whatsnew2020.html#data-class-copy-function-to-have-the-same-visibility-as-constructor
            freeCompilerArgs.addAll("-Xjsr305=strict", "-Xconsistent-data-class-copy-visibility")
            apiVersion.set(KotlinVersion.KOTLIN_2_0)
            jvmTarget.set(JvmTarget.JVM_21)
        }
        jvmToolchain(21)
    }
    tasks.withType<Test> {
        environment("SPRING_PROFILES_ACTIVE", "test")
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }

    // see https://github.com/detekt/detekt/issues/6198
    configurations.matching { it.name == "detekt" }.all {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.jetbrains.kotlin") {
                useVersion("2.0.10")
            }
        }
    }

    tasks.withType<Detekt>().configureEach {
        config.setFrom(files("$rootDir/config/detekt/detekt.yml"))
        buildUponDefaultConfig = true
        baseline.set(file("$projectDir/config/detekt/baseline.xml"))
        source("src/main/kotlin", "src/test/kotlin", "src/testFixtures/kotlin")

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
    jacoco {
        toolVersion = "0.8.9"
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

    project.ext.set("jibFromImage", "eclipse-temurin:21-jre")
    project.ext.set(
        "jibFromPlatforms",
        listOf(
            PlatformParameters().apply { os = "linux"; architecture = "arm64" },
            PlatformParameters().apply { os = "linux"; architecture = "amd64" }
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
            "com.java.version" to "${JavaVersion.VERSION_21}"
        )
    )
}

allprojects {
    group = "com.egm.stellio"
    version = "2.19.0"

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
