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
    implementation("io.r2dbc:r2dbc-postgresql")
    implementation(project(":shared"))

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("com.github.tomakehurst:wiremock-standalone:2.27.2")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation(testFixtures(project(":shared")))
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.to.image = "stellio/stellio-search-service"
jib.container.jvmFlags = project.ext["jibContainerJvmFlags"] as List<String>
jib.container.ports = listOf("8083")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
