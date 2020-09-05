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
    id("org.springframework.boot")
}

dependencies {
    implementation("org.springframework.boot.experimental:spring-boot-actuator-autoconfigure-r2dbc")
    implementation("org.springframework.boot.experimental:spring-boot-starter-data-r2dbc")
    implementation("org.springframework:spring-jdbc")
    // required for Flyway's direct access to the DB to apply migration scripts
    implementation("org.flywaydb:flyway-core")
    implementation("com.jayway.jsonpath:json-path:2.4.0")
    implementation("io.r2dbc:r2dbc-postgresql")
    implementation(project(":shared"))
    // firebase SDK
    implementation("com.google.firebase:firebase-admin:6.12.2")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot.experimental:spring-boot-test-autoconfigure-r2dbc")
    testImplementation("com.github.tomakehurst:wiremock-standalone:2.25.1")
    testImplementation(testFixtures(project(":shared")))
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
jib.to.image = "stellio/stellio-subscription-service"
jib.container.jvmFlags = project.ext["jibContainerJvmFlags"] as List<String>
jib.container.ports = listOf("8084")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()
jib.container.labels = project.ext["jibContainerLabels"] as Map<String, String>
