package com.egm.stellio.search.entity.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.util.asJsonB
import com.egm.stellio.search.common.util.deserializeAsMap
import com.egm.stellio.search.common.util.valueToDoubleOrNull
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Attribute.AttributeType
import com.egm.stellio.search.entity.model.AttributeMetadata
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_LANGUAGE_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSON
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
import com.egm.stellio.shared.model.NGSILD_NULL
import com.egm.stellio.shared.model.NGSILD_PREFIX
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCAB
import com.egm.stellio.shared.model.NgsiLdAttributeInstance
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdJsonPropertyInstance
import com.egm.stellio.shared.model.NgsiLdLanguagePropertyInstance
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.model.NgsiLdRelationshipInstance
import com.egm.stellio.shared.model.NgsiLdVocabPropertyInstance
import com.egm.stellio.shared.model.RelationshipObjects
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.model.getMemberValue
import com.egm.stellio.shared.model.getPropertyValue
import com.egm.stellio.shared.model.getRelationshipId
import com.egm.stellio.shared.model.getRelationshipObjects
import com.egm.stellio.shared.util.ErrorMessages.Entity.NGSI_LD_NULL_NOT_ALLOWED_IN_DATASET_ID_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.Entity.attributeCannotGetValueMessage
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.savvasdalkitsis.jsonmerger.JsonMerger
import io.r2dbc.postgresql.codec.Json
import java.net.URI
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
                Triple(AttributeType.Property, it.first, it.second)
            }

        is NgsiLdRelationshipInstance ->
            guessRelationshipValueType(this.objectId).let {
                Triple(AttributeType.Relationship, it.first, it.second)
            }

        is NgsiLdGeoPropertyInstance ->
            Triple(
                AttributeType.GeoProperty,
                Attribute.AttributeValueType.GEOMETRY,
                Triple(null, null, this.coordinates)
            )
        is NgsiLdJsonPropertyInstance ->
            Triple(
                AttributeType.JsonProperty,
                Attribute.AttributeValueType.JSON,
                Triple(this.json.asJsonB(), null, null)
            )
        is NgsiLdLanguagePropertyInstance ->
            Triple(
                AttributeType.LanguageProperty,
                Attribute.AttributeValueType.ARRAY,
                Triple(this.languageMap.asJsonB(), null, null)
            )
        is NgsiLdVocabPropertyInstance ->
            Triple(
                AttributeType.VocabProperty,
                Attribute.AttributeValueType.ARRAY,
                Triple(this.vocab.asJsonB(), null, null)
            )
    }
    if (attributeValue == Triple(null, null, null)) {
        JsonLdUtils.logger.warn("Unable to get a value from attribute: $this")
        return BadRequestDataException(attributeCannotGetValueMessage(this.toString())).left()
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
    attributeType: AttributeType,
    expandedAttributeInstance: ExpandedAttributeInstance
): Either<APIException, Attribute.AttributeValueType> = either {
    when (attributeType) {
        AttributeType.Property ->
            guessPropertyValueType(expandedAttributeInstance.getPropertyValue().bind()).first
        AttributeType.Relationship ->
            guessRelationshipValueType(expandedAttributeInstance.getRelationshipObjects().bind()).first
        AttributeType.GeoProperty -> Attribute.AttributeValueType.GEOMETRY
        AttributeType.JsonProperty -> Attribute.AttributeValueType.JSON
        AttributeType.LanguageProperty -> Attribute.AttributeValueType.ARRAY
        AttributeType.VocabProperty -> Attribute.AttributeValueType.ARRAY
    }
}

fun guessPropertyValueType(
    ngsiLdPropertyInstance: NgsiLdPropertyInstance
): Pair<Attribute.AttributeValueType, Triple<Json?, Double?, WKTCoordinates?>> =
    guessPropertyValueType(ngsiLdPropertyInstance.value)

fun guessPropertyValueType(
    value: Any
): Pair<Attribute.AttributeValueType, Triple<Json?, Double?, WKTCoordinates?>> =
    when (value) {
        is Double -> Pair(Attribute.AttributeValueType.NUMBER, Triple(null, valueToDoubleOrNull(value), null))
        is Int -> Pair(Attribute.AttributeValueType.NUMBER, Triple(null, valueToDoubleOrNull(value), null))
        is Map<*, *> -> Pair(Attribute.AttributeValueType.OBJECT, Triple(value.asJsonB(), null, null))
        is List<*> -> Pair(Attribute.AttributeValueType.ARRAY, Triple(value.asJsonB(), null, null))
        is String -> Pair(Attribute.AttributeValueType.STRING, Triple(value.asJsonB(), null, null))
        is Boolean -> Pair(Attribute.AttributeValueType.BOOLEAN, Triple(value.asJsonB(), null, null))
        is LocalDate -> Pair(Attribute.AttributeValueType.DATE, Triple(value.toString().asJsonB(), null, null))
        is ZonedDateTime -> Pair(
            Attribute.AttributeValueType.DATETIME,
            Triple(value.toString().asJsonB(), null, null)
        )
        is LocalTime -> Pair(Attribute.AttributeValueType.TIME, Triple(value.toString().asJsonB(), null, null))
        else -> Pair(Attribute.AttributeValueType.STRING, Triple(value.toString().asJsonB(), null, null))
    }

fun guessRelationshipValueType(
    objectId: RelationshipObjects
): Pair<Attribute.AttributeValueType, Triple<Json?, Double?, WKTCoordinates?>> =
    when (objectId) {
        is RelationshipObjects.Single ->
            Pair(Attribute.AttributeValueType.URI, Triple(objectId.id.asJsonB(), null, null))
        is RelationshipObjects.Multiple ->
            Pair(Attribute.AttributeValueType.ARRAY, Triple(objectId.ids.asJsonB(), null, null))
    }

private fun isNgsiLdNullSubAttribute(attrValue: Any): Boolean {
    val instance = (attrValue as? List<*>)?.firstOrNull() as? ExpandedAttributeInstance ?: return false
    val typeUri = (instance[JSONLD_TYPE_KW] as? List<*>)?.firstOrNull() as? String ?: return false
    val attributeType = AttributeType.entries.find { typeUri == "$NGSILD_PREFIX${it.name}" } ?: return false
    return hasNgsiLdNullValue(instance, attributeType)
}

private fun isNgsiLdNullId(attrValue: Any): Boolean =
    ((attrValue as? List<*>)?.firstOrNull() as? Map<*, *>)?.get(JSONLD_ID_KW) == NGSILD_NULL

private fun isNgsiLdNullValue(attrValue: Any): Boolean =
    ((attrValue as? List<*>)?.firstOrNull() as? Map<*, *>)?.get(JSONLD_VALUE_KW) == NGSILD_NULL

private fun mergeLanguageMap(
    source: ExpandedAttributeInstance,
    attrName: String,
    attrValue: List<Any>
): List<Any> {
    val sourceLangEntries = source[attrName] as List<Map<String, String>>
    val targetLangEntries = sourceLangEntries.toMutableList()
    (attrValue as List<Map<String, String>>).forEach { langEntry ->
        targetLangEntries.removeIf { it[JSONLD_LANGUAGE_KW] == langEntry[JSONLD_LANGUAGE_KW] }
        if (langEntry[JSONLD_VALUE_KW] != NGSILD_NULL)
            targetLangEntries.add(langEntry)
    }
    return targetLangEntries
}

/**
 * Removes from the merged map any keys that have an NGSI-LD Null value in the update.
 *
 * Two representations are handled:
 * - JsonProperty: value is `{"@value": {...raw JSON...}, "@type": "@json"}` — null keys are inside the @value map
 * - Property with object value: expanded as nested JSON-LD with IRI keys — null keys are top-level entries whose
 *   single expanded value is `[{"@value": "urn:ngsi-ld:null"}]`
 */
private fun applyNgsiLdNullRemoval(merged: Map<String, Any>, update: Any): Map<String, Any> {
    val updateMap = update as? Map<*, *> ?: return merged

    val jsonContent = updateMap[JSONLD_VALUE_KW] as? Map<String, Any>
    if (jsonContent != null) {
        val nullKeys = jsonContent.filter { (_, v) -> v == NGSILD_NULL }.keys
        val mergedContent = merged[JSONLD_VALUE_KW] as? Map<String, Any>
        return if (nullKeys.isEmpty() || mergedContent == null) merged
        else merged + (JSONLD_VALUE_KW to mergedContent.filterKeys { it !in nullKeys })
    }

    val nullKeys = updateMap.filter { (_, v) ->
        (v as? List<*>)?.singleOrNull()?.let { it as? Map<*, *> }?.get(JSONLD_VALUE_KW) == NGSILD_NULL
    }.keys
    return if (nullKeys.isEmpty()) merged else merged.filterKeys { it !in nullKeys }
}

/**
 * Returns whether the expanded attribute instance holds a NGSI-LD Null value
 */
fun hasNgsiLdNullValue(
    expandedAttributeInstance: ExpandedAttributeInstance,
    attributeType: AttributeType
): Boolean =
    if (attributeType == AttributeType.Relationship) {
        val value = expandedAttributeInstance.getRelationshipId()
        value is URI && value.toString() == NGSILD_NULL
    } else {
        val value = expandedAttributeInstance
            .getMemberValue(attributeType.toExpandedValueMember()).getOrNull()
        value is String && value == NGSILD_NULL
    }

fun Json.toExpandedAttributeInstance(): ExpandedAttributeInstance =
    this.deserializeAsMap() as ExpandedAttributeInstance

fun partialUpdatePatch(
    source: ExpandedAttributeInstance,
    update: ExpandedAttributeInstance
): Either<APIException, Pair<String, ExpandedAttributeInstance>> = either {
    val target = source.toMutableMap()
    update.forEach { (attrName, attrValue) ->
        when {
            attrName == NGSILD_DATASET_ID_IRI && isNgsiLdNullId(attrValue) ->
                raise(BadRequestDataException(NGSI_LD_NULL_NOT_ALLOWED_IN_DATASET_ID_MESSAGE))
            isNgsiLdNullSubAttribute(attrValue) || isNgsiLdNullValue(attrValue) ->
                target.remove(attrName)
            else -> target[attrName] = attrValue
        }
    }
    Pair(JsonUtils.serializeObject(target), target)
}

fun mergePatch(
    source: ExpandedAttributeInstance,
    update: ExpandedAttributeInstance
): Either<APIException, Pair<String, ExpandedAttributeInstance>> = either {
    val target = source.toMutableMap()
    update.forEach { (attrName, attrValue) ->
        when {
            attrName == NGSILD_DATASET_ID_IRI && isNgsiLdNullId(attrValue) ->
                raise(BadRequestDataException(NGSI_LD_NULL_NOT_ALLOWED_IN_DATASET_ID_MESSAGE))
            listOf(NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP).contains(attrName) ->
                target[attrName] = mergeLanguageMap(source, attrName, attrValue)
            isNgsiLdNullSubAttribute(attrValue) || isNgsiLdNullValue(attrValue) ->
                target.remove(attrName)
            !source.containsKey(attrName) ->
                target[attrName] = attrValue
            listOf(
                NGSILD_JSONPROPERTY_JSON,
                NGSILD_VOCABPROPERTY_VOCAB,
                NGSILD_PROPERTY_VALUE
            ).contains(attrName) -> {
                if (attrValue.size > 1) {
                    // a Property holding an array of value or a JsonProperty holding an array of JSON objects
                    // cannot be safely merged patch, so copy the whole value from the update
                    target[attrName] = attrValue
                } else {
                    val mergedElement = JsonMerger().merge(
                        JsonUtils.serializeObject(source[attrName]!![0]),
                        JsonUtils.serializeObject(attrValue[0])
                    ).deserializeAsMap()
                    target[attrName] = listOf(
                        if (attrName == NGSILD_JSONPROPERTY_JSON || attrName == NGSILD_PROPERTY_VALUE)
                            applyNgsiLdNullRemoval(mergedElement, attrValue[0])
                        else
                            mergedElement
                    )
                }
            }
            else -> target[attrName] = attrValue
        }
    }
    Pair(JsonUtils.serializeObject(target), target)
}
