import com.google.cloud.tools.jib.gradle.PlatformParameters

plugins {
    id("com.google.cloud.tools.jib")
    id("org.springframework.boot")
}

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-gateway")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-client")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.from.platforms.addAll(project.ext["jibFromPlatforms"] as List<PlatformParameters>)
jib.to.image = "stellio/stellio-api-gateway"
jib.container.jvmFlags = project.ext["jibContainerJvmFlags"] as List<String>
jib.container.ports = listOf("8080")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
