package com.egm.stellio.shared

import org.testcontainers.containers.DockerComposeContainer
import java.io.File

open class TestContainers(
    val serviceName: String,
    val servicePort: Int
) {
    class KDockerComposeContainer(file: File) : DockerComposeContainer<KDockerComposeContainer>(file)

    private val DOCKER_COMPOSE_FILE = File("docker-compose.yml")
    protected val instance: KDockerComposeContainer by lazy { defineDockerCompose() }

    private fun defineDockerCompose() =
        KDockerComposeContainer(
            DOCKER_COMPOSE_FILE
        ).withLocalCompose(true).withExposedService(serviceName, servicePort)

    fun startContainers() {
        instance.start()
    }
}
