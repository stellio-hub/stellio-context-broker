package com.egm.stellio.subscription.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("application")
data class ApplicationProperties(
    val pagination: Pagination
) {
    data class Pagination(
        val limitDefault: Int,
        val limitMax: Int
    )
}
