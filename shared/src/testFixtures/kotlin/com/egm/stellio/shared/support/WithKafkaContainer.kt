package com.egm.stellio.shared.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

interface WithKafkaContainer {

    companion object {

        private val kafkaImage: DockerImageName = DockerImageName.parse("kymeric/cp-kafka")
            .asCompatibleSubstituteFor("confluentinc/cp-kafka:5.4.1")

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
