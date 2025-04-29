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
    // https://docs.gradle.org/8.4/userguide/upgrading_version_8.html#test_framework_implementation_dependencies
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testFixturesImplementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    testFixturesImplementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    testFixturesImplementation("org.springframework:spring-core")
    testFixturesImplementation("org.springframework.security:spring-security-oauth2-jose")
    testFixturesImplementation("org.springframework.security:spring-security-test")
    testFixturesImplementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    testFixturesImplementation("io.arrow-kt:arrow-fx-coroutines:2.1.1")
    testFixturesImplementation("org.springframework.boot:spring-boot-starter-test") {
        // to ensure we are using mocks and spies from springmockk lib instead
        exclude(module = "mockito-core")
    }
    testFixturesImplementation("org.mock-server:mockserver-netty-no-dependencies:5.15.0")
    testFixturesImplementation("org.mock-server:mockserver-client-java-no-dependencies:5.15.0")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

    testFixturesApi("org.testcontainers:testcontainers")
    testFixturesApi("org.testcontainers:junit-jupiter")
    testFixturesApi("org.testcontainers:kafka")
}
