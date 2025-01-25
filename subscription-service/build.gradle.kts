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
    implementation("org.springframework.boot:spring-boot-starter-oauth2-client")
    // required for Flyway's direct access to the DB to apply migration scripts
    implementation("org.springframework:spring-jdbc")
    implementation("org.flywaydb:flyway-core")
    implementation("org.postgresql:r2dbc-postgresql")
    implementation("com.jayway.jsonpath:json-path:2.9.0")
    implementation(project(":shared"))
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")
    implementation("org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.7")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("io.r2dbc:r2dbc-pool")

    testImplementation("org.wiremock:wiremock-standalone:3.10.0")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation(testFixtures(project(":shared")))
    // https://docs.gradle.org/8.4/userguide/upgrading_version_8.html#test_framework_implementation_dependencies
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.from.platforms.addAll(project.ext["jibFromPlatforms"] as List<PlatformParameters>)
jib.to.image = "stellio/stellio-subscription-service:${project.version}"
jib.pluginExtensions {
    pluginExtension {
        implementation = "com.google.cloud.tools.jib.gradle.extension.springboot.JibSpringBootExtension"
    }
}
jib.container.ports = listOf("8084")
jib.container.creationTime.set(project.ext["jibContainerCreationTime"].toString())
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
