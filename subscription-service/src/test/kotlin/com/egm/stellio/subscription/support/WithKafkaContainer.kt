package com.egm.stellio.subscription.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

interface WithKafkaContainer {

    companion object {

        private val kafkaImage: DockerImageName =
            DockerImageName.parse("confluentinc/cp-kafka:7.3.1")

        private val kafkaContainer = KafkaContainer(kafkaImage).apply {
            withReuse(true)
        }

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
