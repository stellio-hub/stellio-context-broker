import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.reporter.ReporterType

plugins {
    id("org.springframework.boot") version "2.2.1.RELEASE"
    id("io.spring.dependency-management") version "1.0.8.RELEASE"
    kotlin("jvm") version "1.3.50"
    kotlin("plugin.spring") version "1.3.50"
    id("org.jlleitschuh.gradle.ktlint") version "8.2.0"
    id("com.google.cloud.tools.jib") version "1.6.1"
}

group = "com.egm.datahub"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_1_8

val developmentOnly by configurations.creating
configurations {
    runtimeClasspath {
        extendsFrom(developmentOnly)
    }
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

repositories {
    mavenCentral()
    maven { url = uri("https://repo.spring.io/milestone") }
    jcenter()
}
extra["springCloudVersion"] = "Hoxton.RC2"

dependencies {
    developmentOnly("org.springframework.boot:spring-boot-devtools")

    implementation("org.springframework.boot.experimental:spring-boot-actuator-autoconfigure-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot.experimental:spring-boot-starter-data-r2dbc")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("com.beust:klaxon:5.0.1")
    implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka")
    implementation("org.springframework.cloud:spring-cloud-stream")
    implementation("org.flywaydb:flyway-core")
    // required for Flyway's direct access to the DB to apply migration scripts
    implementation("org.springframework:spring-jdbc")

    runtimeOnly("io.r2dbc:r2dbc-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
    }
    testImplementation("org.springframework.boot.experimental:spring-boot-test-autoconfigure-r2dbc")
    testImplementation("com.ninja-squad:springmockk:1.1.3")
    testImplementation("com.github.tomakehurst:wiremock-standalone:2.25.1")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.springframework.cloud:spring-cloud-stream-test-support")
    testImplementation("org.testcontainers:testcontainers:1.12.3")
    testImplementation("org.testcontainers:postgresql:1.12.3")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot.experimental:spring-boot-bom-r2dbc:0.1.0.M3")
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
    }
}

defaultTasks("bootRun")

ktlint {
    verbose.set(true)
    outputToConsole.set(true)
    coloredOutput.set(true)
    reporters.set(setOf(ReporterType.CHECKSTYLE, ReporterType.JSON))
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "1.8"
    }
}

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

tasks.withType<Test> {
    environment("SPRING_PROFILES_ACTIVE", "test")
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

jib {
    from {
        image = "openjdk:alpine"
    }
    to {
        image = "easyglobalmarket/context-search"
    }
    container {
        jvmFlags = listOf("-Xms512m")
        ports = listOf("8083")
        creationTime = "USE_CURRENT_TIMESTAMP"
    }
}
