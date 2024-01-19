package com.egm.stellio.shared.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("application")
data class ApplicationProperties(
    val authentication: Authentication,
    val pagination: Pagination,
    val tenants: List<TenantConfiguration>
) {
    data class Authentication(
        val enabled: Boolean
    )

    data class Pagination(
        val limitDefault: Int,
        val limitMax: Int
    )

    data class TenantConfiguration(
        val name: String,
        val issuer: String,
        val dbSchema: String
    )
}
