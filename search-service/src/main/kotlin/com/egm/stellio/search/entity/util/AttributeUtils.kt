package com.egm.stellio.search.entity.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.common.util.valueToDoubleOrNull
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.AttributeMetadata
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.NgsiLdAttributeInstance
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdJsonPropertyInstance
import com.egm.stellio.shared.model.NgsiLdLanguagePropertyInstance
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.model.NgsiLdRelationshipInstance
import com.egm.stellio.shared.model.NgsiLdVocabPropertyInstance
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.model.getPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.savvasdalkitsis.jsonmerger.JsonMerger
import io.r2dbc.postgresql.codec.Json
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

fun NgsiLdEntity.prepareAttributes(): Either<APIException, List<Pair<String, AttributeMetadata>>> {
    val ngsiLdEntity = this
    return either {
        ngsiLdEntity.attributes
            .flatMap { ngsiLdAttribute ->
                ngsiLdAttribute.getAttributeInstances().map { Pair(ngsiLdAttribute, it) }
            }
            .map {
                Pair(it.first.name, it.second.toAttributeMetadata().bind())
            }
    }
}

fun NgsiLdAttributeInstance.toAttributeMetadata(): Either<APIException, AttributeMetadata> {
    val (attributeType, attributeValueType, attributeValue) = when (this) {
        is NgsiLdPropertyInstance ->
            guessPropertyValueType(this).let {
                Triple(Attribute.AttributeType.Property, it.first, it.second)
            }
        is NgsiLdRelationshipInstance ->
            Triple(
                Attribute.AttributeType.Relationship,
                Attribute.AttributeValueType.URI,
                Triple(this.objectId.toString(), null, null)
            )
        is NgsiLdGeoPropertyInstance ->
            Triple(
                Attribute.AttributeType.GeoProperty,
                Attribute.AttributeValueType.GEOMETRY,
                Triple(null, null, this.coordinates)
            )
        is NgsiLdJsonPropertyInstance ->
            Triple(
                Attribute.AttributeType.JsonProperty,
                Attribute.AttributeValueType.JSON,
                Triple(JsonUtils.serializeObject(this.json), null, null)
            )
        is NgsiLdLanguagePropertyInstance ->
            Triple(
                Attribute.AttributeType.LanguageProperty,
                Attribute.AttributeValueType.ARRAY,
                Triple(JsonUtils.serializeObject(this.languageMap), null, null)
            )
        is NgsiLdVocabPropertyInstance ->
            Triple(
                Attribute.AttributeType.VocabProperty,
                Attribute.AttributeValueType.ARRAY,
                Triple(JsonUtils.serializeObject(this.vocab), null, null)
            )
    }
    if (attributeValue == Triple(null, null, null)) {
        JsonLdUtils.logger.warn("Unable to get a value from attribute: $this")
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
    attributeType: Attribute.AttributeType,
    expandedAttributeInstance: ExpandedAttributeInstance
): Attribute.AttributeValueType =
    when (attributeType) {
        Attribute.AttributeType.Property ->
            guessPropertyValueType(expandedAttributeInstance.getPropertyValue()!!).first
        Attribute.AttributeType.Relationship -> Attribute.AttributeValueType.URI
        Attribute.AttributeType.GeoProperty -> Attribute.AttributeValueType.GEOMETRY
        Attribute.AttributeType.JsonProperty -> Attribute.AttributeValueType.JSON
        Attribute.AttributeType.LanguageProperty -> Attribute.AttributeValueType.ARRAY
        Attribute.AttributeType.VocabProperty -> Attribute.AttributeValueType.ARRAY
    }

fun guessPropertyValueType(
    ngsiLdPropertyInstance: NgsiLdPropertyInstance
): Pair<Attribute.AttributeValueType, Triple<String?, Double?, WKTCoordinates?>> =
    guessPropertyValueType(ngsiLdPropertyInstance.value)

fun guessPropertyValueType(
    value: Any
): Pair<Attribute.AttributeValueType, Triple<String?, Double?, WKTCoordinates?>> =
    when (value) {
        is Double -> Pair(Attribute.AttributeValueType.NUMBER, Triple(null, valueToDoubleOrNull(value), null))
        is Int -> Pair(Attribute.AttributeValueType.NUMBER, Triple(null, valueToDoubleOrNull(value), null))
        is Map<*, *> -> Pair(Attribute.AttributeValueType.OBJECT, Triple(JsonUtils.serializeObject(value), null, null))
        is List<*> -> Pair(Attribute.AttributeValueType.ARRAY, Triple(JsonUtils.serializeObject(value), null, null))
        is String -> Pair(Attribute.AttributeValueType.STRING, Triple(value, null, null))
        is Boolean -> Pair(Attribute.AttributeValueType.BOOLEAN, Triple(value.toString(), null, null))
        is LocalDate -> Pair(Attribute.AttributeValueType.DATE, Triple(value.toString(), null, null))
        is ZonedDateTime -> Pair(Attribute.AttributeValueType.DATETIME, Triple(value.toString(), null, null))
        is LocalTime -> Pair(Attribute.AttributeValueType.TIME, Triple(value.toString(), null, null))
        else -> Pair(Attribute.AttributeValueType.STRING, Triple(value.toString(), null, null))
    }

fun Json.toExpandedAttributeInstance(): ExpandedAttributeInstance =
    this.deserializeAsMap() as ExpandedAttributeInstance

fun partialUpdatePatch(
    source: ExpandedAttributeInstance,
    update: ExpandedAttributeInstance
): Pair<String, ExpandedAttributeInstance> {
    val target = source.plus(update)
    return Pair(JsonUtils.serializeObject(target), target)
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
            listOf(
                JsonLdUtils.NGSILD_JSONPROPERTY_VALUE,
                JsonLdUtils.NGSILD_VOCABPROPERTY_VALUE,
                JsonLdUtils.NGSILD_PROPERTY_VALUE
            ).contains(attrName)
        ) {
            if (attrValue.size > 1) {
                // a Property holding an array of value or a JsonPropery holding an array of JSON objects
                // cannot be safely merged patch, so copy the whole value from the update
                target[attrName] = attrValue
            } else {
                target[attrName] = listOf(
                    JsonMerger().merge(
                        JsonUtils.serializeObject(source[attrName]!![0]),
                        JsonUtils.serializeObject(attrValue[0])
                    ).deserializeAsMap()
                )
            }
        } else if (listOf(JsonLdUtils.NGSILD_LANGUAGEPROPERTY_VALUE).contains(attrName)) {
            val sourceLangEntries = source[attrName] as List<Map<String, String>>
            val targetLangEntries = sourceLangEntries.toMutableList()
            (attrValue as List<Map<String, String>>).forEach { langEntry ->
                // remove any previously existing entry for this language
                targetLangEntries.removeIf {
                    it[JsonLdUtils.JSONLD_LANGUAGE] == langEntry[JsonLdUtils.JSONLD_LANGUAGE]
                }
                targetLangEntries.add(langEntry)
            }

            target[attrName] = targetLangEntries
        } else {
            target[attrName] = attrValue
        }
    }

    return Pair(JsonUtils.serializeObject(target), target)
}
