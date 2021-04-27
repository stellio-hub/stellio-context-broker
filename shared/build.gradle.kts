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
    testFixturesImplementation("org.testcontainers:testcontainers")
    testFixturesImplementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    testFixturesImplementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    testFixturesImplementation("org.springframework:spring-core")
    testFixturesImplementation("org.springframework.security:spring-security-oauth2-jose")
    testFixturesImplementation("org.springframework.security:spring-security-test")
    testFixturesImplementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    testFixturesImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
        // to ensure we are using mocks and spies from springmockk lib instead
        exclude(module = "mockito-core")
    }
    testImplementation("org.hamcrest:hamcrest:2.2")
}
