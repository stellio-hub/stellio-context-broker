val mainClass = "com.egm.stellio.entity.EntityServiceApplicationKt"

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
    implementation("org.springframework.boot:spring-boot-starter-data-neo4j")
    implementation("eu.michael-simons.neo4j:neo4j-migrations-spring-boot-starter:1.1.0")
    implementation(project(":shared"))

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    testImplementation("org.hamcrest:hamcrest:2.2")
    testImplementation("org.testcontainers:neo4j")
    testImplementation(testFixtures(project(":shared")))
}

defaultTasks("bootRun")

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib.from.image = "adoptopenjdk/openjdk11:alpine-jre"
jib.to.image = "stellio/stellio-entity-service"
jib.container.entrypoint = listOf(
    "/bin/sh",
    "-c",
    "/database/wait-for-neo4j.sh neo4j:7687 -t \$NEO4J_WAIT_TIMEOUT -- " +
        "java " +
        (project.ext["jibContainerJvmFlags"] as List<String>).joinToString(" ") +
        " -cp /app/resources:/app/classes:/app/libs/* " + mainClass
)
jib.container.environment = mapOf("NEO4J_WAIT_TIMEOUT" to "100")
jib.container.ports = listOf("8082")
jib.container.creationTime = project.ext["jibContainerCreationTime"].toString()
jib.container.labels.putAll(project.ext["jibContainerLabels"] as Map<String, String>)
jib.extraDirectories.permissions = mapOf("/database/wait-for-neo4j.sh" to "775")
