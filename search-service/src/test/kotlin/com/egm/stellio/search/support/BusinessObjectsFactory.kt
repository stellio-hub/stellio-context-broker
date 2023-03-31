package com.egm.stellio.search.support

import com.egm.stellio.search.model.*
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
    val attributeMetadata = AttributeMetadata(
        measuredValue = Random.nextDouble(),
        value = null,
        geoValue = null,
        valueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
        datasetId = null,
        type = TemporalEntityAttribute.AttributeType.Property,
        observedAt = Instant.now().atZone(ZoneOffset.UTC)
    )
    val payload = JsonLdUtils.buildExpandedProperty(attributeMetadata.measuredValue!!)
        .addSubAttribute(
            JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY,
            JsonLdUtils.buildNonReifiedDateTime(attributeMetadata.observedAt!!)
        )
        .getSingleEntry()

    return AttributeInstance(
        temporalEntityAttribute = teaUuid,
        attributeMetadata = attributeMetadata,
        timeProperty = timeProperty,
        payload = payload
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
