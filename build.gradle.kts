import io.gitlab.arturbosch.detekt.detekt
import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.reporter.ReporterType

val detektConfigFile = file("$rootDir/config/detekt/detekt.yml")

extra["springCloudVersion"] = "2020.0.4"
extra["testcontainersVersion"] = "1.16.0"

plugins {
    java // why did I have to add that ?!
    // only apply the plugin in the subprojects requiring it because it expects a Spring Boot app
    // and the shared lib is obviously not one
    id("org.springframework.boot") version "2.5.7" apply false
    id("io.spring.dependency-management") version "1.0.11.RELEASE" apply false
    kotlin("jvm") version "1.5.31" apply false
    kotlin("plugin.spring") version "1.5.31" apply false
    id("org.jlleitschuh.gradle.ktlint") version "10.2.0"
    id("com.google.cloud.tools.jib") version "3.1.4" apply false
    id("io.gitlab.arturbosch.detekt") version "1.19.0" apply false
    id("org.sonarqube") version "3.3"
    jacoco
}

subprojects {
    repositories {
        mavenCentral()
    }

    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.spring")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "io.gitlab.arturbosch.detekt")
    apply(plugin = "jacoco")

    java.sourceCompatibility = JavaVersion.VERSION_11

    the<DependencyManagementExtension>().apply {
        imports {
            mavenBom("org.testcontainers:testcontainers-bom:${property("testcontainersVersion")}")
            mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
        }
    }

    dependencies {
        implementation("org.jetbrains.kotlin:kotlin-reflect")
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
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
        implementation("com.github.jsonld-java:jsonld-java:0.13.3")

        implementation("io.arrow-kt:arrow-fx-coroutines:1.0.1")

        implementation("org.locationtech.jts.io:jts-io-common:1.18.1")

        "detektPlugins"("io.gitlab.arturbosch.detekt:detekt-formatting:1.18.0")

        annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

        runtimeOnly("de.siegmar:logback-gelf:4.0.0")
        runtimeOnly("io.micrometer:micrometer-registry-prometheus")

        testImplementation("org.springframework.boot:spring-boot-starter-test") {
            // to ensure we are using mocks and spies from springmockk lib instead
            exclude(module = "mockito-core")
        }
        testImplementation("com.ninja-squad:springmockk:3.0.1")
        testImplementation("io.mockk:mockk:1.12.0")
        testImplementation("io.projectreactor:reactor-test")
        testImplementation("org.springframework.security:spring-security-test")
        testImplementation("org.testcontainers:testcontainers")
        testImplementation("org.testcontainers:junit-jupiter")
        testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions {
            freeCompilerArgs = listOf("-Xjsr305=strict")
            jvmTarget = "11"
        }
    }
    tasks.withType<Test> {
        environment("SPRING_PROFILES_ACTIVE", "test")
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }

    ktlint {
        disabledRules.set(setOf("experimental:multiline-if-else", "no-wildcard-imports"))
        reporters {
            reporter(ReporterType.CHECKSTYLE)
            reporter(ReporterType.PLAIN)
        }
    }

    detekt {
        toolVersion = "1.18.0"
        source = files("src/main/kotlin", "src/test/kotlin")
        config = files(detektConfigFile)
        buildUponDefaultConfig = true
        baseline = file("$projectDir/config/detekt/baseline.xml")

        reports {
            xml.enabled = true
            txt.enabled = false
            html.enabled = true
        }
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

    project.ext.set("jibFromImage", "adoptopenjdk:11-jre")
    project.ext.set("jibContainerJvmFlags", listOf("-Xms256m", "-Xmx768m"))
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
            "com.java.version" to "${JavaVersion.VERSION_11}"
        )
    )
}

allprojects {
    group = "com.egm.stellio"
    version = "1.4.0-dev"

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
