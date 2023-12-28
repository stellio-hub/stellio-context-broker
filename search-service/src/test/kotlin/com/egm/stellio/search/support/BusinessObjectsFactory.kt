package com.egm.stellio.search.support

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.addNonReifiedTemporalProperty
import com.egm.stellio.shared.model.getSingleEntry
import com.egm.stellio.shared.util.*
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
        observedAt = ngsiLdDateTime()
    )
    val payload = JsonLdUtils.buildExpandedPropertyValue(attributeMetadata.measuredValue!!)
        .addNonReifiedTemporalProperty(JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY, attributeMetadata.observedAt!!)
        .getSingleEntry()

    return AttributeInstance(
        temporalEntityAttribute = teaUuid,
        time = attributeMetadata.observedAt!!,
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
        entitiesQuery = EntitiesQuery(
            paginationQuery = PaginationQuery(limit = 50, offset = 0),
            context = APIC_COMPOUND_CONTEXT
        ),
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        withAggregatedValues = withAggregatedValues
    )

fun buildDefaultQueryParams(): EntitiesQuery =
    EntitiesQuery(
        paginationQuery = PaginationQuery(limit = 50, offset = 0),
        context = APIC_COMPOUND_CONTEXT
    )
