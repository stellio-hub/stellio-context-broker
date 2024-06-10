package com.egm.stellio.subscription.support

import com.egm.stellio.subscription.service.mqtt.Mqtt
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

interface WithMosquittoContainer {

    companion object {

        private val mosquittoImage: DockerImageName =
            DockerImageName.parse("eclipse-mosquitto:2.0.18")

        private val mosquittoContainer = GenericContainer<Nothing>(mosquittoImage).apply {
            withReuse(true)
            withExposedPorts(Mqtt.SCHEME.MQTT_DEFAULT_PORT)
            withCopyFileToContainer(
                MountableFile.forClasspathResource("/mosquitto/mosquitto.conf"),
                "/mosquitto/config/mosquitto.conf"
            )
        }

        fun getPort(): Int = mosquittoContainer.getMappedPort(
            Mqtt.SCHEME.MQTT_DEFAULT_PORT
        )

        init {
            mosquittoContainer.start()
        }

        fun getMosquittoLogs(): String = mosquittoContainer.logs
    }
}
