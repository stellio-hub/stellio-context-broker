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
    implementation("org.json:json:20230618")
    implementation(project(":shared"))

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.1")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation(testFixtures(project(":shared")))
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
jib.container.jvmFlags = listOf("-Xms512m", "-Xmx1024m")
jib.container.ports = listOf("8083")
jib.container.creationTime.set(project.ext["jibContainerCreationTime"].toString())
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
