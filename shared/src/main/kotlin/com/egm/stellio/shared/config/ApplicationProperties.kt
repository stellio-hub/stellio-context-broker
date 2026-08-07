package com.egm.stellio.shared.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

private const val DEFAULT_TRANSACTION_TIMEOUT_SECONDS = 50L

@ConfigurationProperties("application")
data class ApplicationProperties(
    val authentication: Authentication,
    val pagination: Pagination,
    val tenants: List<TenantConfiguration>,
    val contexts: Contexts,
    val transactionTimeout: Duration = Duration.ofSeconds(DEFAULT_TRANSACTION_TIMEOUT_SECONDS)
) {

    data class Authentication(
        val enabled: Boolean,
        val claimsPaths: List<String>
    )

    data class Pagination(
        val limitDefault: Int,
        val limitMax: Int,
        val temporalLimit: Int
    )

    data class TenantConfiguration(
        val name: String,
        val dbSchema: String,
        val issuer: String? = null,
        val clientId: String? = null,
        val clientSecret: String? = null,
        // use Keycloak path as default path
        val accessTokenURL: String? = issuer?.trimEnd('/').plus("/protocol/openid-connect/token")
    )

    data class Contexts(
        val core: String,
        val authz: String,
        val authzCompound: String
    )
}
