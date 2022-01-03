package com.egm.stellio.shared.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@Testcontainers
interface WithKafkaContainer {
    companion object {

        private val kafkaImage: DockerImageName = DockerImageName.parse("confluentinc/cp-kafka:5.4.1")

        @Container
        val kafkaContainer = KafkaContainer(kafkaImage)

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafkaContainer.bootstrapServers }
        }

        init {
            kafkaContainer.start()
        }
    }
}
