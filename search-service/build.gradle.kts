configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

plugins {
    id("org.springframework.boot")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    // required for Flyway's direct access to the DB to apply migration scripts
    // (https://github.com/flyway/flyway/issues/2502)
    implementation("org.springframework:spring-jdbc")
    // implementation (and not runtime) because we are using the native jsonb encoding provided by PG
    implementation("org.postgresql:r2dbc-postgresql")
    implementation("com.github.stellio-hub:json-merge:0.1.0")
    implementation("org.json:json:20251224")
    implementation(project(":shared"))

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("io.r2dbc:r2dbc-pool")

    testImplementation("org.wiremock:wiremock-standalone:3.13.2")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers-r2dbc")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation(testFixtures(project(":shared")))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

tasks.bootBuildImage {
    imageName = "stellio/stellio-search-service:${project.version}"
    imagePlatform = "linux/amd64,linux/arm64"

    val buildpackEnvironment = project.ext["buildpackEnvironment"] as? Map<String, String> ?: emptyMap()
    val buildpackRuntimeEnvironment = project.ext["buildpackRuntimeEnvironment"] as? Map<String, String> ?: emptyMap()
    val buildpackOciLabels = project.ext["buildpackOciLabels"] as? Map<String, String> ?: emptyMap()
    environment = buildpackEnvironment
        .plus(buildpackRuntimeEnvironment)
        .plus(buildpackOciLabels)
}
