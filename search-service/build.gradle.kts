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
    implementation("org.springframework.boot:spring-boot-starter-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    // required for Flyway's direct access to the DB to apply migration scripts
    // (https://github.com/flyway/flyway/issues/2502)
    implementation("org.springframework:spring-jdbc")
    // implementation (and not runtime) because we are using the native jsonb encoding provided by PG
    implementation("org.postgresql:r2dbc-postgresql")
    // implementation (and not runtime) because DatabaseTenantConfig binds Micrometer gauges directly
    // against io.r2dbc.pool.ConnectionPool's PoolMetrics
    implementation("io.r2dbc:r2dbc-pool")
    implementation(project(":shared"))

    // perf-test only: detects blocking calls on Reactor/Netty non-blocking threads. Inert unless
    // BLOCKHOUND_ENABLED=true (see SearchServiceApplication.main) - reconsider keeping this
    // dependency before merging to develop.
    implementation("io.projectreactor.tools:blockhound:1.0.11.RELEASE")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")

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

// perf-test only: BlockHound's self-attach needs the jdk.attach module, which the -jre jlink image
// below excludes ("No compatible attachment provider is available" at startup otherwise). Swap to
// the full JDK image only for a BlockHound run, keyed off the same BLOCKHOUND_ENABLED flag used at
// runtime so this reverts automatically once that's unset.
jib.from.image =
    if (System.getenv("BLOCKHOUND_ENABLED") == "true")
        project.ext["jibFromImage"].toString().replace("-jre", "")
    else
        project.ext["jibFromImage"].toString()
jib.from.platforms.addAll(project.ext["jibFromPlatforms"] as List<PlatformParameters>)
jib.to.image = "stellio/stellio-search-service:${project.version}"
jib.container.ports = listOf("8083")
jib.container.creationTime.set(project.ext["jibContainerCreationTime"].toString())
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
