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
    implementation("org.springframework:spring-jdbc")
    implementation("org.flywaydb:flyway-core")
    implementation("org.postgresql:r2dbc-postgresql")
    implementation("com.jayway.jsonpath:json-path:2.7.0")
    implementation(project(":shared"))
    // firebase SDK
    implementation("com.google.firebase:firebase-admin:9.0.0")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-client")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.21.0")

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
jib.from.platforms.addAll(project.ext["jibFromPlatforms"] as List<PlatformParameters>)
jib.to.image = "stellio/stellio-subscription-service"
jib.pluginExtensions {
    pluginExtension {
        implementation = "com.google.cloud.tools.jib.gradle.extension.springboot.JibSpringBootExtension"
    }
}
jib.container.jvmFlags = project.ext["jibContainerJvmFlags"] as List<String>
jib.container.ports = listOf("8084")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
