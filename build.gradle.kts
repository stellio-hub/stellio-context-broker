import io.gitlab.arturbosch.detekt.detekt
import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.reporter.ReporterType

val detektConfigFile = file("$rootDir/config/detekt/detekt.yml")

extra["springCloudVersion"] = "Hoxton.SR6"

plugins {
    java // why did I have to add that ?!
    // only apply the plugin in the subprojects requiring it because it expects a Spring Boot app
    // and the shared lib is obviously not one
    id("org.springframework.boot") version "2.2.9.RELEASE" apply false
    id("io.spring.dependency-management") version "1.0.9.RELEASE" apply false
    kotlin("jvm") version "1.3.72" apply false
    kotlin("plugin.spring") version "1.3.72" apply false
    id("org.jlleitschuh.gradle.ktlint") version "9.3.0"
    id("com.google.cloud.tools.jib") version "2.5.0" apply false
    kotlin("kapt") version "1.3.61" apply false
    id("io.gitlab.arturbosch.detekt") version "1.11.2" apply false
}

subprojects {
    repositories {
        mavenCentral()
        maven { url = uri("https://repo.spring.io/milestone") }
        jcenter()
        maven { url = uri("https://dl.bintray.com/arrow-kt/arrow-kt/") }
    }

    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.spring")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "kotlin-kapt")
    apply(plugin = "io.gitlab.arturbosch.detekt")

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

        implementation("io.arrow-kt:arrow-fx:0.10.4")
        implementation("io.arrow-kt:arrow-syntax:0.10.4")
        "kapt"("io.arrow-kt:arrow-meta:0.10.4")

        "detektPlugins"("io.gitlab.arturbosch.detekt:detekt-formatting:1.11.2")

        annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

        runtimeOnly("de.siegmar:logback-gelf:3.0.0")
        runtimeOnly("io.micrometer:micrometer-registry-prometheus")

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
        enableExperimentalRules.set(true)
        disabledRules.set(setOf("experimental:multiline-if-else", "no-wildcard-imports"))
        reporters {
            reporter(ReporterType.CHECKSTYLE)
            reporter(ReporterType.PLAIN)
        }
    }

    detekt {
        toolVersion = "1.11.2"
        input = files("src/main/kotlin", "src/test/kotlin")
        config = files(detektConfigFile)
        buildUponDefaultConfig = true
        baseline = file("$projectDir/config/detekt/baseline.xml")

        reports {
            xml.enabled = true
            txt.enabled = false
            html.enabled = true
        }
    }

    project.ext.set("jibFromImage", "gcr.io/distroless/java:11")
    project.ext.set("jibContainerJvmFlag", "-Xms512m")
    project.ext.set("jibContainerCreationTime", "USE_CURRENT_TIMESTAMP")
}

allprojects {
    group = "com.egm.stellio"
    version = "0.7.0"

    repositories {
        mavenCentral()
        maven { url = uri("https://repo.spring.io/milestone") }
        jcenter()
    }
}
