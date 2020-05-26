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
    implementation("org.springframework.boot:spring-boot-starter-data-neo4j")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.neo4j:neo4j-ogm-bolt-native-types")
    implementation("org.liquigraph:liquigraph-spring-boot-starter:4.0.1")
    implementation("org.jgrapht:jgrapht-core:1.4.0")

    implementation(project(":shared"))

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    testImplementation("org.hamcrest:hamcrest:2.1")
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = project.ext["jibFromImage"].toString()
jib.to.image = "stellio/stellio-entity-service"
jib.container.jvmFlags = listOf(project.ext["jibContainerJvmFlag"].toString())
jib.container.ports = listOf("8082")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()
