package com.egm.stellio.search.util

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.logger

fun valueToDoubleOrNull(value: Any): Double? =
    when (value) {
        is Double -> value
        is Int -> value.toDouble()
        else -> null
    }

fun valueToStringOrNull(value: Any): String? =
    when (value) {
        is String -> value
        is Boolean -> value.toString()
        else -> null
    }

suspend fun NgsiLdEntity.prepareTemporalAttributes(): Either<APIException, List<Pair<String, AttributeMetadata>>> {
    val ngsiLdEntity = this
    return either {
        ngsiLdEntity.attributes
            .flatMap { ngsiLdAttribute ->
                ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) }
            }
            .map {
                Pair(it.first.name, it.second.toTemporalAttributeMetadata().bind())
            }
    }
}

fun NgsiLdAttributeInstance.toTemporalAttributeMetadata(): Either<APIException, AttributeMetadata> {
    val attributeType = when (this) {
        is NgsiLdPropertyInstance -> TemporalEntityAttribute.AttributeType.Property
        is NgsiLdRelationshipInstance -> TemporalEntityAttribute.AttributeType.Relationship
        is NgsiLdGeoPropertyInstance -> TemporalEntityAttribute.AttributeType.GeoProperty
    }
    val attributeValue = when (this) {
        is NgsiLdPropertyInstance ->
            Triple(
                valueToStringOrNull(this.value),
                valueToDoubleOrNull(this.value),
                null
            )
        is NgsiLdRelationshipInstance -> Triple(this.objectId.toString(), null, null)
        is NgsiLdGeoPropertyInstance -> Triple(null, null, this.coordinates)
    }
    if (attributeValue == Triple(null, null, null)) {
        logger.warn("Unable to get a value from attribute: $this")
        return BadRequestDataException("Unable to get a value from attribute: $this").left()
    }
    val attributeValueType =
        if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
        else if (attributeValue.third != null) TemporalEntityAttribute.AttributeValueType.GEOMETRY
        else TemporalEntityAttribute.AttributeValueType.ANY

    return AttributeMetadata(
        measuredValue = attributeValue.second,
        value = attributeValue.first,
        geoValue = attributeValue.third,
        valueType = attributeValueType,
        datasetId = this.datasetId,
        type = attributeType,
        observedAt = this.observedAt
    ).right()
}
