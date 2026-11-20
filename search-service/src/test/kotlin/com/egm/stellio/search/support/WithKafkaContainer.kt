package com.egm.stellio.search.support

import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.kafka.ConfluentKafkaContainer

// started once per JVM, like WithTimescaleContainer: a per-class container restarts on a new port, while cached
// Spring contexts keep pointing at the old one (KafkaHealthService then rejects every write)
@Suppress("UtilityClassWithPublicConstructor")
open class WithKafkaContainer {

    companion object {

        @ServiceConnection
        @JvmStatic
        val kafkaContainer = ConfluentKafkaContainer("confluentinc/cp-kafka:8.3.2").apply {
            withReuse(true)
            start()
        }
    }
}
