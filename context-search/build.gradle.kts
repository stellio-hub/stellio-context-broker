val developmentOnly by configurations.creating
configurations {
    runtimeClasspath {
        extendsFrom(developmentOnly)
    }
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

plugins {
    id("com.google.cloud.tools.jib")
}

dependencies {
    implementation("org.springframework.boot.experimental:spring-boot-actuator-autoconfigure-r2dbc")
    implementation("org.springframework.boot.experimental:spring-boot-starter-data-r2dbc")
    implementation("org.springframework:spring-jdbc")
    // required for Flyway's direct access to the DB to apply migration scripts
    implementation("org.flywaydb:flyway-core")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("io.r2dbc:r2dbc-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot.experimental:spring-boot-test-autoconfigure-r2dbc")
    testImplementation("org.testcontainers:testcontainers:1.12.3")
    testImplementation("org.testcontainers:postgresql:1.12.3")
    testImplementation("com.github.tomakehurst:wiremock-standalone:2.25.1")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot.experimental:spring-boot-bom-r2dbc:0.1.0.M3")
    }
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.to.image = "easyglobalmarket/context-search"
jib.container.jvmFlags = listOf(project.ext["jibContainerJvmFlag"].toString())
jib.container.ports = listOf("8083")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()