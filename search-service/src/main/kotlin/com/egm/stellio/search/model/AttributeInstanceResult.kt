package com.egm.stellio.search.model

import java.net.URI
import java.time.ZonedDateTime

open class AttributeInstanceResult(
    open val attributeName: String,
    open val datasetId: URI? = null,
    open val value: Any,
    open val observedAt: ZonedDateTime
)

data class FullAttributeInstanceResult(
    override val attributeName: String,
    val instanceId: URI? = null,
    override val datasetId: URI? = null,
    override val value: Any,
    override val observedAt: ZonedDateTime,
    val payload: String
) : AttributeInstanceResult(attributeName, datasetId, value, observedAt)

data class SimplifiedAttributeInstanceResult(
    override val attributeName: String,
    override val datasetId: URI? = null,
    override val value: Any,
    override val observedAt: ZonedDateTime
) : AttributeInstanceResult(attributeName, datasetId, value, observedAt)
