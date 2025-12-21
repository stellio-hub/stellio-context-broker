import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension

tasks.jar {
    archiveBaseName.set("stellio-context-broker-shared")
}

plugins {
    id("java-test-fixtures")
}

// https://docs.spring.io/spring-boot/docs/2.2.5.RELEASE/gradle-plugin/reference/html/#managing-dependencies-using-in-isolation
the<DependencyManagementExtension>().apply {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
    }
}

dependencies {
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testFixturesImplementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    testFixturesImplementation("tools.jackson.module:jackson-module-kotlin:3.0.+")
    testFixturesImplementation("org.springframework:spring-core")
    testFixturesImplementation("org.springframework.security:spring-security-oauth2-jose")
    testFixturesImplementation("org.springframework.security:spring-security-test")
    testFixturesImplementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    testFixturesImplementation("org.springframework.boot:spring-boot-starter-test")
    testFixturesImplementation("io.arrow-kt:arrow-fx-coroutines:2.2.0")
    testFixturesImplementation("org.wiremock:wiremock-standalone:3.13.2")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

    testFixturesApi("org.testcontainers:testcontainers")
    testFixturesApi("org.testcontainers:testcontainers-junit-jupiter")
    testFixturesApi("org.testcontainers:testcontainers-kafka")
}
