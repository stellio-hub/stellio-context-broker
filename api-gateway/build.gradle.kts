plugins {
    id("org.springframework.boot")
}

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-gateway-server-webflux")
    implementation("org.springframework.boot:spring-boot-starter-webclient")
    implementation("org.zalando:logbook-spring-boot-webflux-autoconfigure:4.0.0-RC.1")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.8")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

springBoot {
    buildInfo {
        properties {
            name.set("Stellio Context Broker")
        }
    }
}

tasks.bootBuildImage {
    imageName = "stellio/stellio-api-gateway:${project.version}"
    builder = project.ext["buildpackBuilder"] as String
    buildpacks = listOf("paketobuildpacks/java")
    imagePlatform = "linux/amd64"

    val buildpackEnvironment = project.ext["buildpackEnvironment"] as? Map<String, String> ?: emptyMap()
    val buildpackRuntimeEnvironment = project.ext["buildpackRuntimeEnvironment"] as? Map<String, String> ?: emptyMap()
    val buildpackOciLabels = project.ext["buildpackOciLabels"] as? Map<String, String> ?: emptyMap()
    environment = buildpackEnvironment
        .plus(buildpackRuntimeEnvironment)
        .plus(buildpackOciLabels)
}
