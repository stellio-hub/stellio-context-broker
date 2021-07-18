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
    implementation("org.springframework:spring-jdbc")
    implementation("org.flywaydb:flyway-core")
    implementation("io.r2dbc:r2dbc-postgresql")
    implementation("com.jayway.jsonpath:json-path:2.6.0")
    implementation(project(":shared"))
    // firebase SDK
    implementation("com.google.firebase:firebase-admin:6.12.2")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("com.github.tomakehurst:wiremock-standalone:2.25.1")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation(testFixtures(project(":shared")))
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.to.image = "stellio/stellio-subscription-service"
jib.container.jvmFlags = project.ext["jibContainerJvmFlags"] as List<String>
jib.container.ports = listOf("8084")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()
jib.container.labels = project.ext["jibContainerLabels"] as Map<String, String>
