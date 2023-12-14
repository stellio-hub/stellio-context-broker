import com.google.cloud.tools.jib.gradle.PlatformParameters

plugins {
    id("com.google.cloud.tools.jib")
    id("org.springframework.boot")
}

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-gateway")
    implementation("org.zalando:logbook-spring-boot-webflux-autoconfigure:3.6.0")

    // https://docs.gradle.org/8.4/userguide/upgrading_version_8.html#test_framework_implementation_dependencies
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    detektPlugins("io.gitlab.arturbosch.detekt:detekt-formatting:1.23.4")
}

springBoot {
    buildInfo {
        properties {
            name.set("Stellio Context Broker")
        }
    }
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.from.platforms.addAll(project.ext["jibFromPlatforms"] as List<PlatformParameters>)
jib.to.image = "stellio/stellio-api-gateway:${project.version}"
jib.container.jvmFlags = listOf("-Xms64m", "-Xmx128m")
jib.container.ports = listOf("8080")
jib.container.creationTime.set(project.ext["jibContainerCreationTime"].toString())
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
