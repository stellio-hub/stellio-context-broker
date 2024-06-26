package com.egm.stellio.search.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalEntityAttribute.AttributeValueType
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_LANGUAGE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LANGUAGEPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_VOCABPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.savvasdalkitsis.jsonmerger.JsonMerger
import io.r2dbc.postgresql.codec.Json
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
        is NgsiLdJsonPropertyInstance ->
            Triple(
                TemporalEntityAttribute.AttributeType.JsonProperty,
                AttributeValueType.JSON,
                Triple(serializeObject(this.json), null, null)
            )
        is NgsiLdLanguagePropertyInstance ->
            Triple(
                TemporalEntityAttribute.AttributeType.LanguageProperty,
                AttributeValueType.ARRAY,
                Triple(serializeObject(this.languageMap), null, null)
            )
        is NgsiLdVocabPropertyInstance ->
            Triple(
                TemporalEntityAttribute.AttributeType.VocabProperty,
                AttributeValueType.ARRAY,
                Triple(serializeObject(this.vocab), null, null)
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
            guessPropertyValueType(expandedAttributeInstance.getPropertyValue()!!).first
        TemporalEntityAttribute.AttributeType.Relationship -> AttributeValueType.URI
        TemporalEntityAttribute.AttributeType.GeoProperty -> AttributeValueType.GEOMETRY
        TemporalEntityAttribute.AttributeType.JsonProperty -> AttributeValueType.JSON
        TemporalEntityAttribute.AttributeType.LanguageProperty -> AttributeValueType.ARRAY
        TemporalEntityAttribute.AttributeType.VocabProperty -> AttributeValueType.ARRAY
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
        else -> Pair(AttributeValueType.STRING, Triple(value.toString(), null, null))
    }

fun Json.toExpandedAttributeInstance(): ExpandedAttributeInstance =
    this.deserializeAsMap() as ExpandedAttributeInstance

fun partialUpdatePatch(
    source: ExpandedAttributeInstance,
    update: ExpandedAttributeInstance
): Pair<String, ExpandedAttributeInstance> {
    val target = source.plus(update)
    return Pair(serializeObject(target), target)
}

fun mergePatch(
    source: ExpandedAttributeInstance,
    update: ExpandedAttributeInstance
): Pair<String, ExpandedAttributeInstance> {
    val target = source.toMutableMap()
    update.forEach { (attrName, attrValue) ->
        if (!source.containsKey(attrName)) {
            target[attrName] = attrValue
        } else if (
            listOf(NGSILD_JSONPROPERTY_VALUE, NGSILD_VOCABPROPERTY_VALUE, NGSILD_PROPERTY_VALUE).contains(attrName)
        ) {
            if (attrValue.size > 1) {
                // a Property holding an array of value or a JsonPropery holding an array of JSON objects
                // cannot be safely merged patch, so copy the whole value from the update
                target[attrName] = attrValue
            } else {
                target[attrName] = listOf(
                    JsonMerger().merge(
                        serializeObject(source[attrName]!![0]),
                        serializeObject(attrValue[0])
                    ).deserializeAsMap()
                )
            }
        } else if (listOf(NGSILD_LANGUAGEPROPERTY_VALUE).contains(attrName)) {
            val sourceLangEntries = source[attrName] as List<Map<String, String>>
            val targetLangEntries = sourceLangEntries.toMutableList()
            (attrValue as List<Map<String, String>>).forEach { langEntry ->
                // remove any previously existing entry for this language
                targetLangEntries.removeIf {
                    it[JSONLD_LANGUAGE] == langEntry[JSONLD_LANGUAGE]
                }
                targetLangEntries.add(langEntry)
            }

            target[attrName] = targetLangEntries
        } else {
            target[attrName] = attrValue
        }
    }

    return Pair(serializeObject(target), target)
}
