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
    implementation("org.springframework.boot:spring-boot-starter-data-neo4j")
    implementation("org.neo4j:neo4j-ogm-bolt-native-types")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    testImplementation("org.hamcrest:hamcrest:2.1")

    testRuntimeOnly("org.neo4j:neo4j-ogm-embedded-driver")
    testRuntimeOnly("org.neo4j:neo4j-ogm-embedded-native-types")
    testRuntimeOnly("org.neo4j:neo4j:3.5.12")
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.to.image = "easyglobalmarket/context-registry"
jib.container.jvmFlags = listOf(project.ext["jibContainerJvmFlag"].toString())
jib.container.ports = listOf("8082")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()