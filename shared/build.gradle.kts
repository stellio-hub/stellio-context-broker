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
    testFixturesImplementation("org.testcontainers:testcontainers:1.12.3")
    testFixturesImplementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    testFixturesImplementation("org.springframework:spring-core")
    testFixturesImplementation("org.springframework.security:spring-security-oauth2-jose")
    testFixturesImplementation("org.springframework.security:spring-security-test")
    testFixturesImplementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    testImplementation("org.hamcrest:hamcrest:2.1")
}
