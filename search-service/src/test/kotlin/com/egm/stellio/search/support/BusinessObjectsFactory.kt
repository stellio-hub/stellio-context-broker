package com.egm.stellio.search.support

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.AttributeMetadata
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_IRI
import com.egm.stellio.shared.model.addNonReifiedTemporalProperty
import com.egm.stellio.shared.model.getSingleEntry
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.DateUtils.ngsiLdDateTime
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.UriUtils.toUri
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.random.Random

fun gimmeEntityPayload(
    entityId: String,
    types: List<ExpandedTerm> = listOf(BEEHIVE_IRI),
    payload: String = EMPTY_PAYLOAD
): Entity =
    gimmeEntityPayload(entityId.toUri(), types, payload)

fun gimmeEntityPayload(
    entityId: URI,
    types: List<ExpandedTerm> = listOf(BEEHIVE_IRI),
    payload: String = EMPTY_PAYLOAD
): Entity =
    Entity(
        entityId = entityId,
        types = types,
        createdAt = ngsiLdDateTime(),
        payload = Json.of(payload)
    )

fun gimmeNumericPropertyAttributeInstance(
    attributeUuid: UUID,
    timeProperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
    measuredValue: Double? = Random.nextDouble(),
    value: String? = null,
    time: ZonedDateTime = ngsiLdDateTime(),
    sub: Sub? = null
): AttributeInstance {
    val attributeMetadata = AttributeMetadata(
        measuredValue = measuredValue,
        value = value,
        geoValue = null,
        valueType = Attribute.AttributeValueType.NUMBER,
        datasetId = null,
        type = Attribute.AttributeType.Property,
        observedAt = time
    )
    val payload = JsonLdUtils.buildExpandedPropertyValue(attributeMetadata.measuredValue!!)
        .addNonReifiedTemporalProperty(NGSILD_OBSERVED_AT_IRI, attributeMetadata.observedAt!!)
        .getSingleEntry()

    return AttributeInstance(
        attributeUuid = attributeUuid,
        time = attributeMetadata.observedAt,
        attributeMetadata = attributeMetadata,
        timeProperty = timeProperty,
        payload = payload,
        sub = sub
    )
}

fun gimmeJsonPropertyAttributeInstance(
    attributeUuid: UUID,
    timeProperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
): AttributeInstance {
    val attributeMetadata = AttributeMetadata(
        measuredValue = null,
        value = SAMPLE_JSON_PROPERTY_PAYLOAD.asString(),
        geoValue = null,
        valueType = Attribute.AttributeValueType.JSON,
        datasetId = null,
        type = Attribute.AttributeType.JsonProperty,
        observedAt = ngsiLdDateTime()
    )
    val payload = JsonLdUtils.buildExpandedPropertyValue(attributeMetadata.value!!)
        .addNonReifiedTemporalProperty(NGSILD_OBSERVED_AT_IRI, attributeMetadata.observedAt!!)
        .getSingleEntry()

    return AttributeInstance(
        attributeUuid = attributeUuid,
        time = attributeMetadata.observedAt,
        attributeMetadata = attributeMetadata,
        timeProperty = timeProperty,
        payload = payload
    )
}

fun gimmeLanguagePropertyAttributeInstance(
    attributeUuid: UUID,
    timeProperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
): AttributeInstance {
    val attributeMetadata = AttributeMetadata(
        measuredValue = null,
        value = SAMPLE_LANGUAGE_PROPERTY_PAYLOAD.asString(),
        geoValue = null,
        valueType = Attribute.AttributeValueType.OBJECT,
        datasetId = null,
        type = Attribute.AttributeType.LanguageProperty,
        observedAt = ngsiLdDateTime()
    )
    val payload = JsonLdUtils.buildExpandedPropertyValue(attributeMetadata.value!!)
        .addNonReifiedTemporalProperty(NGSILD_OBSERVED_AT_IRI, attributeMetadata.observedAt!!)
        .getSingleEntry()

    return AttributeInstance(
        attributeUuid = attributeUuid,
        time = attributeMetadata.observedAt,
        attributeMetadata = attributeMetadata,
        timeProperty = timeProperty,
        payload = payload
    )
}

fun gimmeVocabPropertyAttributeInstance(
    attributeUuid: UUID,
    timeProperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
): AttributeInstance {
    val attributeMetadata = AttributeMetadata(
        measuredValue = null,
        value = SAMPLE_VOCAB_PROPERTY_PAYLOAD.asString(),
        geoValue = null,
        valueType = Attribute.AttributeValueType.ARRAY,
        datasetId = null,
        type = Attribute.AttributeType.VocabProperty,
        observedAt = ngsiLdDateTime()
    )
    val payload = JsonLdUtils.buildExpandedPropertyValue(attributeMetadata.value!!)
        .addNonReifiedTemporalProperty(NGSILD_OBSERVED_AT_IRI, attributeMetadata.observedAt!!)
        .getSingleEntry()

    return AttributeInstance(
        attributeUuid = attributeUuid,
        time = attributeMetadata.observedAt,
        attributeMetadata = attributeMetadata,
        timeProperty = timeProperty,
        payload = payload
    )
}

fun gimmeTemporalEntitiesQuery(
    temporalQuery: TemporalQuery,
    temporalRepresentation: TemporalRepresentation = TemporalRepresentation.NORMALIZED,
    withAudit: Boolean = false
): TemporalEntitiesQueryFromGet =
    TemporalEntitiesQueryFromGet(
        entitiesQuery = EntitiesQueryFromGet(
            paginationQuery = PaginationQuery(limit = 50, offset = 0),
            contexts = APIC_COMPOUND_CONTEXTS
        ),
        temporalQuery = temporalQuery,
        temporalRepresentation = temporalRepresentation,
        withAudit = withAudit
    )

fun buildDefaultQueryParams(): EntitiesQueryFromGet =
    EntitiesQueryFromGet(
        paginationQuery = PaginationQuery(limit = 50, offset = 0),
        contexts = APIC_COMPOUND_CONTEXTS
    )

fun gimmeSucceededAttributeOperationResult(
    attributeName: String = "",
    datasetId: URI? = null,
    newExpandedValue: ExpandedAttributeInstance = emptyMap(),
): SucceededAttributeOperationResult =
    SucceededAttributeOperationResult(
        attributeName,
        datasetId,
        OperationStatus.CREATED,
        newExpandedValue
    )
