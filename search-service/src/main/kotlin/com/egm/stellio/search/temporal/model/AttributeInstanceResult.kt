package com.egm.stellio.search.temporal.model

import java.time.ZonedDateTime
import java.util.UUID

sealed class AttributeInstanceResult(open val attributeUuid: UUID) : Comparable<AttributeInstanceResult> {
    abstract fun getComparableTime(): ZonedDateTime

    override fun compareTo(other: AttributeInstanceResult): Int =
        this.getComparableTime().compareTo(other.getComparableTime())
}

data class FullAttributeInstanceResult(
    override val attributeUuid: UUID,
    val payload: String,
    val time: ZonedDateTime,
    val timeproperty: String,
    val sub: String?
) : AttributeInstanceResult(attributeUuid) {
    override fun getComparableTime(): ZonedDateTime = time
}

data class SimplifiedAttributeInstanceResult(
    override val attributeUuid: UUID,
    val value: Any,
    val time: ZonedDateTime
) : AttributeInstanceResult(attributeUuid) {
    override fun getComparableTime(): ZonedDateTime = time
}

data class AggregatedAttributeInstanceResult(
    override val attributeUuid: UUID,
    val values: List<AggregateResult>
) : AttributeInstanceResult(attributeUuid) {
    override fun getComparableTime(): ZonedDateTime = values.first().startDateTime

    data class AggregateResult(
        val aggregate: TemporalQuery.Aggregate,
        val value: Any,
        val startDateTime: ZonedDateTime,
        val endDateTime: ZonedDateTime
    )
}
