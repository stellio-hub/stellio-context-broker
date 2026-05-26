package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import java.net.URI
import java.time.ZonedDateTime

data class TelemetryDataMessage(
    val tenantName: String = DEFAULT_TENANT_NAME,
    val entityId: URI,
    val attributeName: ExpandedTerm,
    val datasetId: URI? = null,
    val value: Any,
    val observedAt: ZonedDateTime
)
