package com.egm.stellio.shared.util

import com.egm.stellio.shared.web.DEFAULT_TENANT_URI
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import kotlinx.coroutines.reactor.awaitSingle
import reactor.core.publisher.Mono
import java.net.URI

suspend fun getTenantFromContext(): URI =
    Mono.deferContextual { contextView ->
        val tenantUri = contextView.getOrDefault(NGSILD_TENANT_HEADER, DEFAULT_TENANT_URI)!!
        Mono.just(tenantUri)
    }.awaitSingle()
