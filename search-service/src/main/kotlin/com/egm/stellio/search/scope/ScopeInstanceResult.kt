package com.egm.stellio.search.scope

import com.egm.stellio.search.model.TemporalQuery
import java.net.URI
import java.time.ZonedDateTime

sealed class ScopeInstanceResult(open val entityId: URI) : Comparable<ScopeInstanceResult> {
    abstract fun getComparableTime(): ZonedDateTime

    override fun compareTo(other: ScopeInstanceResult): Int =
        this.getComparableTime().compareTo(other.getComparableTime())
}

data class FullScopeInstanceResult(
    override val entityId: URI,
    val scopes: List<String>,
    val time: ZonedDateTime,
    val timeproperty: String,
    val sub: String? = null
) : ScopeInstanceResult(entityId) {
    override fun getComparableTime(): ZonedDateTime = time
}

data class SimplifiedScopeInstanceResult(
    override val entityId: URI,
    val scopes: List<String>,
    val time: ZonedDateTime
) : ScopeInstanceResult(entityId) {
    override fun getComparableTime(): ZonedDateTime = time
}

data class AggregatedScopeInstanceResult(
    override val entityId: URI,
    val values: List<AggregateResult>
) : ScopeInstanceResult(entityId) {
    override fun getComparableTime(): ZonedDateTime = values.first().startDateTime

    data class AggregateResult(
        val aggregate: TemporalQuery.Aggregate,
        val value: Any,
        val startDateTime: ZonedDateTime,
        val endDateTime: ZonedDateTime
    )
}
