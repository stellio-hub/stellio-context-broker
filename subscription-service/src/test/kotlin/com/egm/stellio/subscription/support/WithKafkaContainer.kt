package com.egm.stellio.subscription.support

import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.ConfluentKafkaContainer

@Testcontainers
@Suppress("UtilityClassWithPublicConstructor")
open class WithKafkaContainer {

    companion object {

        @Container
        @ServiceConnection
        @JvmStatic
        val kafkaContainer = ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.0").apply {
            withReuse(true)
        }
    }
}
