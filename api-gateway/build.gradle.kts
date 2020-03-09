plugins {
    id("com.google.cloud.tools.jib")
}

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-gateway")
    implementation("org.springframework.cloud:spring-cloud-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-client")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.to.image = "easyglobalmarket/stellio-api-gateway"
jib.container.jvmFlags = listOf(project.ext["jibContainerJvmFlag"].toString())
jib.container.ports = listOf("8080")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()