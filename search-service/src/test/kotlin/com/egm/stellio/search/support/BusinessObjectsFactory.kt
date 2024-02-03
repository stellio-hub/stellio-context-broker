package com.egm.stellio.search.support

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.addNonReifiedTemporalProperty
import com.egm.stellio.shared.model.getSingleEntry
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.util.UUID
import kotlin.random.Random

fun gimmeEntityPayload(
    entityId: String,
    types: List<ExpandedTerm> = listOf(BEEHIVE_TYPE),
    payload: String = EMPTY_PAYLOAD
): EntityPayload =
    gimmeEntityPayload(entityId.toUri(), types, payload)

fun gimmeEntityPayload(
    entityId: URI,
    types: List<ExpandedTerm> = listOf(BEEHIVE_TYPE),
    payload: String = EMPTY_PAYLOAD
): EntityPayload =
    EntityPayload(
        entityId = entityId,
        types = types,
        createdAt = ngsiLdDateTime(),
        payload = Json.of(payload)
    )

fun gimmeNumericPropertyAttributeInstance(
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

fun gimmeJsonPropertyAttributeInstance(
    teaUuid: UUID,
    timeProperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
): AttributeInstance {
    val attributeMetadata = AttributeMetadata(
        measuredValue = null,
        value = SAMPLE_JSON_PROPERTY_PAYLOAD.asString(),
        geoValue = null,
        valueType = TemporalEntityAttribute.AttributeValueType.JSON,
        datasetId = null,
        type = TemporalEntityAttribute.AttributeType.JsonProperty,
        observedAt = ngsiLdDateTime()
    )
    val payload = JsonLdUtils.buildExpandedPropertyValue(attributeMetadata.value!!)
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
            contexts = APIC_COMPOUND_CONTEXTS
        ),
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        withAggregatedValues = withAggregatedValues
    )

fun buildDefaultQueryParams(): EntitiesQuery =
    EntitiesQuery(
        paginationQuery = PaginationQuery(limit = 50, offset = 0),
        contexts = APIC_COMPOUND_CONTEXTS
    )
