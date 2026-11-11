package com.egm.stellio.search.common.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

private const val DEFAULT_KAFKA_HEALTH_INTERVAL_SECONDS = 5L
private const val DEFAULT_KAFKA_HEALTH_TIMEOUT_SECONDS = 2L
private const val DEFAULT_ENTITY_EVENTS_QUEUE_CAPACITY = 20_000

@ConfigurationProperties("search")
data class SearchProperties(
    val onOwnerDeleteCascadeEntities: Boolean,
    val timezoneForTimeBuckets: String = "GMT",
    val kafkaHealth: KafkaHealth = KafkaHealth(),
    val onKafkaUnavailable: OnKafkaUnavailable = OnKafkaUnavailable.WARN,
    val entityEventsQueueCapacity: Int = DEFAULT_ENTITY_EVENTS_QUEUE_CAPACITY
) {

    enum class OnKafkaUnavailable {
        // accept the write and add an NGSILD-Warning header to the response
        WARN,

        // reject the write with a 500 InternalError, nothing is stored
        FAIL
    }

    data class KafkaHealth(
        val interval: Duration = Duration.ofSeconds(DEFAULT_KAFKA_HEALTH_INTERVAL_SECONDS),
        val timeout: Duration = Duration.ofSeconds(DEFAULT_KAFKA_HEALTH_TIMEOUT_SECONDS)
    )
}
