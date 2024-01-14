package com.egm.stellio.search.tenant

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.NonexistentTenantException
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
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
                Mono.error(NonexistentTenantException("Tenant $tenantName does not exist"))
            else
                Mono.just(tenantName)
        }
}
