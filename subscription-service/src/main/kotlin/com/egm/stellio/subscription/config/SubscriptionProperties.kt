package com.egm.stellio.subscription.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("subscription")
data class SubscriptionProperties(
    val stellioUrl: String
)
