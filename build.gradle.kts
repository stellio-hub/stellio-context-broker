import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.reporter.ReporterType

extra["springCloudVersion"] = "Hoxton.SR2"

plugins {
    java // why did I have to add that ?!
    // only apply the plugin in the subprojects requiring it because it expects a Spring Boot app
    // and the shared lib is obviously not one
    id("org.springframework.boot") version "2.2.5.RELEASE" apply false
    id("io.spring.dependency-management") version "1.0.9.RELEASE" apply false
    kotlin("jvm") version "1.3.61" apply false
    kotlin("plugin.spring") version "1.3.61" apply false
    id("org.jlleitschuh.gradle.ktlint") version "8.2.0"
    id("com.google.cloud.tools.jib") version "1.6.1" apply false
}

subprojects {
    repositories {
        mavenCentral()
        maven { url = uri("https://repo.spring.io/milestone") }
        jcenter()
    }

    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.spring")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")

    java.sourceCompatibility = JavaVersion.VERSION_11

    the<DependencyManagementExtension>().apply {
        imports {
            mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
        }
    }

    dependencies {
        implementation("org.jetbrains.kotlin:kotlin-reflect")
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
        implementation("io.projectreactor.kotlin:reactor-kotlin-extensions:1.0.1.RELEASE")

        implementation("org.springframework.boot:spring-boot-starter-actuator")
        implementation("org.springframework.boot:spring-boot-starter-webflux")
        implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
        implementation("org.springframework.boot:spring-boot-starter-security")
        // it provides support for JWT decoding and verification
        implementation("org.springframework.security:spring-security-oauth2-jose")
        implementation("org.springframework.cloud:spring-cloud-stream")
        implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka")
        implementation("org.springframework.kafka:spring-kafka")

        implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
        implementation("com.github.jsonld-java:jsonld-java:0.13.0")

        annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

        runtimeOnly("de.siegmar:logback-gelf:3.0.0")

        testImplementation("org.springframework.boot:spring-boot-starter-test") {
            exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
            // to ensure we are using mocks and spies from springmockk lib instead
            exclude(module = "mockito-core")
        }
        testImplementation("com.ninja-squad:springmockk:2.0.0")
        testImplementation("io.projectreactor:reactor-test")
        testImplementation("org.springframework.cloud:spring-cloud-stream-test-support")
        testImplementation("org.springframework.security:spring-security-test")
        testImplementation("org.testcontainers:testcontainers:1.12.3")
    }

    tasks.withType<KotlinCompile> {
        kotlinOptions {
            freeCompilerArgs = listOf("-Xjsr305=strict")
            jvmTarget = "1.8"
        }
    }
    tasks.withType<Test> {
        environment("SPRING_PROFILES_ACTIVE", "test")
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }

    project.ext.set("jibFromImage", "adoptopenjdk/openjdk11:alpine-jre")
    project.ext.set("jibContainerJvmFlag", "-Xms512m")
    project.ext.set("jibContainerCreationTime", "USE_CURRENT_TIMESTAMP")
}

allprojects {
    group = "com.egm.stellio"
    version = "0.5.0"

    repositories {
        mavenCentral()
        maven { url = uri("https://repo.spring.io/milestone") }
        jcenter()
    }
}

ktlint {
    verbose.set(true)
    outputToConsole.set(true)
    coloredOutput.set(true)
    reporters.set(setOf(ReporterType.CHECKSTYLE, ReporterType.JSON))
}
