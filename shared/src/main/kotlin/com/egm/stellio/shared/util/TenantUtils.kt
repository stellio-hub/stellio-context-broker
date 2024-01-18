package com.egm.stellio.shared.util

import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import kotlinx.coroutines.reactor.awaitSingle
import reactor.core.publisher.Mono

suspend fun getTenantFromContext(): String =
    Mono.deferContextual { contextView ->
        val tenantName = contextView.getOrDefault(NGSILD_TENANT_HEADER, DEFAULT_TENANT_NAME)!!
        Mono.just(tenantName)
    }.awaitSingle()
