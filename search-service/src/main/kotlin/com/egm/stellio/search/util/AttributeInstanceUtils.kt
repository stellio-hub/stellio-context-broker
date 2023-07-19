package com.egm.stellio.search.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalEntityAttribute.AttributeValueType
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.ExpandedAttributeInstance
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

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

fun NgsiLdEntity.prepareTemporalAttributes(): Either<APIException, List<Pair<String, AttributeMetadata>>> {
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
    val (attributeType, attributeValueType, attributeValue) = when (this) {
        is NgsiLdPropertyInstance ->
            guessPropertyValueType(this).let {
                Triple(TemporalEntityAttribute.AttributeType.Property, it.first, it.second)
            }
        is NgsiLdRelationshipInstance ->
            Triple(
                TemporalEntityAttribute.AttributeType.Relationship,
                AttributeValueType.URI,
                Triple(this.objectId.toString(), null, null)
            )
        is NgsiLdGeoPropertyInstance ->
            Triple(
                TemporalEntityAttribute.AttributeType.GeoProperty,
                AttributeValueType.GEOMETRY,
                Triple(null, null, this.coordinates)
            )
    }
    if (attributeValue == Triple(null, null, null)) {
        logger.warn("Unable to get a value from attribute: $this")
        return BadRequestDataException("Unable to get a value from attribute: $this").left()
    }

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

fun guessAttributeValueType(
    attributeType: TemporalEntityAttribute.AttributeType,
    expandedAttributeInstance: ExpandedAttributeInstance
): AttributeValueType =
    when (attributeType) {
        TemporalEntityAttribute.AttributeType.Property ->
            guessPropertyValueType(expandedAttributeInstance.getPropertyValue()).first
        TemporalEntityAttribute.AttributeType.Relationship -> AttributeValueType.URI
        TemporalEntityAttribute.AttributeType.GeoProperty -> AttributeValueType.GEOMETRY
    }

fun guessPropertyValueType(
    ngsiLdPropertyInstance: NgsiLdPropertyInstance
): Pair<AttributeValueType, Triple<String?, Double?, WKTCoordinates?>> =
    guessPropertyValueType(ngsiLdPropertyInstance.value)

fun guessPropertyValueType(
    value: Any
): Pair<AttributeValueType, Triple<String?, Double?, WKTCoordinates?>> =
    when (value) {
        is Double -> Pair(AttributeValueType.NUMBER, Triple(null, valueToDoubleOrNull(value), null))
        is Int -> Pair(AttributeValueType.NUMBER, Triple(null, valueToDoubleOrNull(value), null))
        is Map<*, *> -> Pair(AttributeValueType.OBJECT, Triple(serializeObject(value), null, null))
        is List<*> -> Pair(AttributeValueType.ARRAY, Triple(serializeObject(value), null, null))
        is String -> Pair(AttributeValueType.STRING, Triple(value, null, null))
        is Boolean -> Pair(AttributeValueType.BOOLEAN, Triple(value.toString(), null, null))
        is LocalDate -> Pair(AttributeValueType.DATE, Triple(value.toString(), null, null))
        is ZonedDateTime -> Pair(AttributeValueType.DATETIME, Triple(value.toString(), null, null))
        is LocalTime -> Pair(AttributeValueType.TIME, Triple(value.toString(), null, null))
        is URI -> Pair(AttributeValueType.URI, Triple(value.toString(), null, null))
        else -> Pair(AttributeValueType.STRING, Triple(value.toString(), null, null))
    }
