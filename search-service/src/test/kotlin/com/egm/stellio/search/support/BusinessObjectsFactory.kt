package com.egm.stellio.search.support

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.addSubAttribute
import com.egm.stellio.shared.util.getSingleEntry
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlin.random.Random

fun gimmeAttributeInstance(
    teaUuid: UUID,
    timeProperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
): AttributeInstance {
    val measuredValue = Random.nextDouble()
    val observedAt = Instant.now().atZone(ZoneOffset.UTC)
    return AttributeInstance(
        temporalEntityAttribute = teaUuid,
        measuredValue = measuredValue,
        timeProperty = timeProperty,
        time = observedAt,
        payload = JsonLdUtils.buildExpandedProperty(measuredValue)
            .addSubAttribute(JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY, JsonLdUtils.buildNonReifiedDateTime(observedAt))
            .getSingleEntry()
    )
}

fun gimmeTemporalEntitiesQuery(
    temporalQuery: TemporalQuery,
    withTemporalValues: Boolean = false,
    withAudit: Boolean = false,
    withAggregatedValues: Boolean = false
): TemporalEntitiesQuery =
    TemporalEntitiesQuery(
        queryParams = QueryParams(limit = 50, offset = 0, context = APIC_COMPOUND_CONTEXT),
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        withAggregatedValues = withAggregatedValues
    )
