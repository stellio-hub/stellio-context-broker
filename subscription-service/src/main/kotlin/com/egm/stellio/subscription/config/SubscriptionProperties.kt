package com.egm.stellio.subscription.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

private const val DEFAULT_LINKED_ENTITIES_TIMEOUT_SECONDS = 5L
private const val DEFAULT_LINKED_ENTITIES_RETRY_MAX_ATTEMPTS = 8L
private const val DEFAULT_LINKED_ENTITIES_RETRY_MIN_BACKOFF_SECONDS = 1L
private const val DEFAULT_LINKED_ENTITIES_RETRY_MAX_BACKOFF_SECONDS = 30L

@ConfigurationProperties("subscription")
data class SubscriptionProperties(
    val stellioUrl: String,
    val linkedEntities: LinkedEntitiesProperties = LinkedEntitiesProperties()
) {
    data class LinkedEntitiesProperties(
        val timeout: Duration = Duration.ofSeconds(DEFAULT_LINKED_ENTITIES_TIMEOUT_SECONDS),
        val retryMaxAttempts: Long = DEFAULT_LINKED_ENTITIES_RETRY_MAX_ATTEMPTS,
        val retryMinBackoff: Duration = Duration.ofSeconds(DEFAULT_LINKED_ENTITIES_RETRY_MIN_BACKOFF_SECONDS),
        val retryMaxBackoff: Duration = Duration.ofSeconds(DEFAULT_LINKED_ENTITIES_RETRY_MAX_BACKOFF_SECONDS)
    )
}
