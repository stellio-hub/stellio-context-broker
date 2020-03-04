import org.jlleitschuh.gradle.ktlint.reporter.ReporterType

val developmentOnly by configurations.creating
configurations {
    runtimeClasspath {
        extendsFrom(developmentOnly)
    }
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-neo4j")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions:1.0.1.RELEASE")
    implementation("org.springframework.cloud:spring-cloud-stream")
    implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    implementation("org.springframework.boot:spring-boot-starter-security")
    // it provides support for JWT decoding and verification
    implementation("org.springframework.security:spring-security-oauth2-jose")
    implementation("org.neo4j:neo4j-ogm-bolt-native-types")
    implementation("com.github.jsonld-java:jsonld-java:0.13.0")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.hamcrest:hamcrest:2.1")
    testImplementation("com.ninja-squad:springmockk:1.1.3")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.springframework.cloud:spring-cloud-stream-test-support")
    testImplementation("org.springframework.security:spring-security-test")

    testRuntime("org.neo4j:neo4j-ogm-embedded-driver")
    testRuntime("org.neo4j:neo4j-ogm-embedded-native-types")
    testRuntime("org.neo4j:neo4j:3.5.12")
}

defaultTasks("bootRun")

ktlint {
    verbose.set(true)
    outputToConsole.set(true)
    coloredOutput.set(true)
    reporters.set(setOf(ReporterType.CHECKSTYLE, ReporterType.JSON))
}

tasks.bootRun {
    environment("SPRING_PROFILES_ACTIVE", "dev")
}

jib {
    from {
        image = "adoptopenjdk/openjdk11:alpine-jre"
    }
    to {
        image = "easyglobalmarket/context-registry"
    }
    container {
        jvmFlags = listOf("-Xms512m")
        ports = listOf("8082")
        creationTime = "USE_CURRENT_TIMESTAMP"
    }
}
