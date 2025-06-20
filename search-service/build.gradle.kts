import com.google.cloud.tools.jib.gradle.PlatformParameters

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

plugins {
    id("com.google.cloud.tools.jib")
    id("org.springframework.boot")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    // required for Flyway's direct access to the DB to apply migration scripts
    // (https://github.com/flyway/flyway/issues/2502)
    implementation("org.springframework:spring-jdbc")
    implementation("org.flywaydb:flyway-core")
    // implementation (and not runtime) because we are using the native jsonb encoding provided by PG
    implementation("org.postgresql:r2dbc-postgresql")
    implementation("com.github.stellio-hub:json-merge:0.1.0")
    implementation("org.json:json:20250517")
    implementation(project(":shared"))

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("io.r2dbc:r2dbc-pool")

    testImplementation("org.wiremock:wiremock-standalone:3.13.1")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation(testFixtures(project(":shared")))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.from.platforms.addAll(project.ext["jibFromPlatforms"] as List<PlatformParameters>)
jib.to.image = "stellio/stellio-search-service:${project.version}"
jib.pluginExtensions {
    pluginExtension {
        implementation = "com.google.cloud.tools.jib.gradle.extension.springboot.JibSpringBootExtension"
    }
}
jib.container.ports = listOf("8083")
jib.container.creationTime.set(project.ext["jibContainerCreationTime"].toString())
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
