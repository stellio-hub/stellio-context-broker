package com.egm.stellio.shared.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.net.URI

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
        val uri: URI,
        val issuer: String,
        val dbSchema: String
    )
}
