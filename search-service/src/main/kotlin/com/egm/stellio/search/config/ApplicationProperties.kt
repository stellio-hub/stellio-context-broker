package com.egm.stellio.search.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("application")
data class ApplicationProperties(
    val authentication: Authentication,
    val pagination: Pagination
) {
    data class Authentication(
        val enabled: Boolean
    )

    data class Pagination(
        val limitDefault: Int,
        val limitMax: Int
    )
}
