
import com.google.cloud.tools.jib.gradle.PlatformParameters
import io.gitlab.arturbosch.detekt.Detekt
import io.gitlab.arturbosch.detekt.DetektCreateBaselineTask
import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    dependencies {
        classpath("com.google.cloud.tools:jib-spring-boot-extension-gradle:0.1.0")
    }
}

extra["springCloudVersion"] = "2022.0.2"

plugins {
    // https://docs.spring.io/spring-boot/docs/current/gradle-plugin/reference/htmlsingle/#reacting-to-other-plugins.java
    java
    `kotlin-dsl`
    // only apply the plugin in the subprojects requiring it because it expects a Spring Boot app
    // and the shared lib is obviously not one
    id("org.springframework.boot") version "3.1.4" apply false
    id("io.spring.dependency-management") version "1.1.3" apply false
    id("org.graalvm.buildtools.native") version "0.9.27"
    kotlin("jvm") version "1.9.10" apply false
    kotlin("plugin.spring") version "1.9.10" apply false
    id("com.google.cloud.tools.jib") version "3.4.0" apply false
    id("io.gitlab.arturbosch.detekt") version "1.23.1" apply false
    id("org.sonarqube") version "4.4.1.3373"
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

    java.sourceCompatibility = JavaVersion.VERSION_17

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
        // it provides support for JWT decoding and verification
        implementation("org.springframework.security:spring-security-oauth2-jose")

        implementation("org.springframework.kafka:spring-kafka")

        implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
        implementation("com.github.jsonld-java:jsonld-java:0.13.4")

        implementation("io.arrow-kt:arrow-fx-coroutines:1.2.1")

        implementation("org.locationtech.jts.io:jts-io-common:1.19.0")

        annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

        runtimeOnly("de.siegmar:logback-gelf:4.0.2")
        runtimeOnly("io.micrometer:micrometer-registry-prometheus")

        testImplementation("org.springframework.boot:spring-boot-starter-test") {
            // to ensure we are using mocks and spies from springmockk (and not from Mockito)
            exclude(module = "mockito-core")
        }
        testImplementation("com.ninja-squad:springmockk:4.0.2")
        testImplementation("io.projectreactor:reactor-test")
        testImplementation("org.springframework.security:spring-security-test")
        testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions {
            freeCompilerArgs = listOf("-Xjsr305=strict", "-opt-in=kotlin.RequiresOptIn")
            jvmTarget = "${JavaVersion.VERSION_17}"
        }
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
                useVersion("1.9.0")
            }
        }
    }

    tasks.withType<Detekt>().configureEach {
        config.setFrom(files("$rootDir/config/detekt/detekt.yml"))
        buildUponDefaultConfig = true
        baseline.set(file("$projectDir/config/detekt/baseline.xml"))

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
        toolVersion = "0.8.7"
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

    project.ext.set("jibFromImage", "eclipse-temurin:17-jre")
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
            "com.java.version" to "${JavaVersion.VERSION_17}"
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
