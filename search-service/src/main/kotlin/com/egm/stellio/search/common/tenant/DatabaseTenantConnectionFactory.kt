package com.egm.stellio.search.common.tenant

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.NonexistentTenantException
import com.egm.stellio.shared.util.ErrorMessages.Tenant.tenantNotFoundMessage
import com.egm.stellio.shared.util.NGSILD_TENANT_HEADER
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import org.springframework.r2dbc.connection.lookup.AbstractRoutingConnectionFactory
import reactor.core.publisher.Mono

class DatabaseTenantConnectionFactory(
    private val applicationProperties: ApplicationProperties
) : AbstractRoutingConnectionFactory() {

    override fun determineCurrentLookupKey(): Mono<Any> =
        // when working out of a web context (e.g., in tests), nothing is set from the tenant header
        // so set here also the default tenant name
        Mono.deferContextual { contextView ->
            val tenantName = contextView.getOrDefault(NGSILD_TENANT_HEADER, DEFAULT_TENANT_NAME)!!
            if (!applicationProperties.tenants.map { it.name }.contains(tenantName))
                Mono.error(NonexistentTenantException(tenantNotFoundMessage(tenantName)))
            else
                Mono.just(tenantName)
        }
}
