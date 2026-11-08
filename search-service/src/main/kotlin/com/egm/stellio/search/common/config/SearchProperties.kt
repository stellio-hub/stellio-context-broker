package com.egm.stellio.search.common.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

private const val DEFAULT_KAFKA_HEALTH_INTERVAL_SECONDS = 5L
private const val DEFAULT_KAFKA_HEALTH_TIMEOUT_SECONDS = 2L

@ConfigurationProperties("search")
data class SearchProperties(
    val onOwnerDeleteCascadeEntities: Boolean,
    val timezoneForTimeBuckets: String = "GMT",
    val kafkaHealth: KafkaHealth = KafkaHealth()
) {

    data class KafkaHealth(
        val interval: Duration = Duration.ofSeconds(DEFAULT_KAFKA_HEALTH_INTERVAL_SECONDS),
        val timeout: Duration = Duration.ofSeconds(DEFAULT_KAFKA_HEALTH_TIMEOUT_SECONDS)
    )
}
