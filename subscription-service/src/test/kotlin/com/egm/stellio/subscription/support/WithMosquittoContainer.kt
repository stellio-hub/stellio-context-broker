package com.egm.stellio.subscription.support

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

interface WithMosquittoContainer {

    companion object {

        private val mosquittoImage: DockerImageName =
            DockerImageName.parse("eclipse-mosquitto:2.0.18")

        val mosquittoContainer = GenericContainer<Nothing>(mosquittoImage).apply {
            withReuse(true)
            withExposedPorts(1883)
            withCopyFileToContainer(
                MountableFile.forClasspathResource("/mosquitto/mosquitto.conf"),
                "/mosquitto/config/mosquitto.conf"
            )
        }

        init {
            mosquittoContainer.start()
        }
    }
}
