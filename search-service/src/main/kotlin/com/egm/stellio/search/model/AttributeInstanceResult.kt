package com.egm.stellio.search.model

import java.net.URI
import java.time.ZonedDateTime

data class AttributeInstanceResult(
    val attributeName: String,
    val instanceId: URI? = null,
    val datasetId: URI? = null,
    val value: Any? = null,
    val observedAt: ZonedDateTime
)
