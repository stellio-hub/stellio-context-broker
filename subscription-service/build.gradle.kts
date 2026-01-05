configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

plugins {
    id("org.springframework.boot")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-security-oauth2-client")
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    // required for Flyway's direct access to the DB to apply migration scripts
    implementation("org.springframework:spring-jdbc")
    implementation("org.postgresql:r2dbc-postgresql")
    implementation("com.jayway.jsonpath:json-path:2.10.0")
    implementation(project(":shared"))
    implementation("org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5")
    implementation("org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    runtimeOnly("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("io.r2dbc:r2dbc-pool")

    testImplementation("org.wiremock:wiremock-standalone:3.13.2")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers-r2dbc")
    testImplementation(testFixtures(project(":shared")))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

tasks.bootBuildImage {
    imageName = "stellio/stellio-subscription-service:${project.version}"
    imagePlatform = "linux/amd64"

    val buildpackEnvironment = project.ext["buildpackEnvironment"] as? Map<String, String> ?: emptyMap()
    val buildpackRuntimeEnvironment = project.ext["buildpackRuntimeEnvironment"] as? Map<String, String> ?: emptyMap()
    val buildpackOciLabels = project.ext["buildpackOciLabels"] as? Map<String, String> ?: emptyMap()
    environment = buildpackEnvironment
        .plus(buildpackRuntimeEnvironment)
        .plus(buildpackOciLabels)
}
