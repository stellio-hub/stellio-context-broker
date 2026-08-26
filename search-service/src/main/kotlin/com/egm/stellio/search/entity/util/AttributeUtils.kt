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
import com.egm.stellio.shared.model.ExpandedAttributeValue
import com.egm.stellio.shared.model.ExpandedLanguageMapValue
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_LANGUAGE_KW
import com.egm.stellio.shared.model.JSONLD_LIST_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSON
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
import com.egm.stellio.shared.model.NGSILD_NULL
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCAB
import com.egm.stellio.shared.model.NgsiLdAttributeInstance
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdGeoPropertyInstance
import com.egm.stellio.shared.model.NgsiLdJsonPropertyInstance
import com.egm.stellio.shared.model.NgsiLdLanguagePropertyInstance
import com.egm.stellio.shared.model.NgsiLdListPropertyInstance
import com.egm.stellio.shared.model.NgsiLdListRelationshipInstance
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
        is NgsiLdListPropertyInstance ->
            Triple(
                AttributeType.ListProperty,
                Attribute.AttributeValueType.ARRAY,
                Triple(this.valueList.asJsonB(), null, null)
            )
        is NgsiLdListRelationshipInstance ->
            Triple(
                AttributeType.ListRelationship,
                Attribute.AttributeValueType.ARRAY,
                Triple(this.objectList.asJsonB(), null, null)
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
        observedAt = this.observedAt,
        expiresAt = this.expiresAt
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
        AttributeType.ListProperty -> Attribute.AttributeValueType.ARRAY
        AttributeType.ListRelationship -> Attribute.AttributeValueType.ARRAY
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

private fun isNgsiLdNullSubAttribute(attrValue: List<Any>): Boolean {
    val instance = attrValue.firstOrNull() as? ExpandedAttributeInstance ?: return false
    val typeUri = (instance[JSONLD_TYPE_KW] as? List<*>)?.firstOrNull() as? String ?: return false
    val attributeType = AttributeType.entries.find { typeUri == it.toExpandedName() } ?: return false
    return hasNgsiLdNullValue(instance, attributeType)
}

private fun isNgsiLdNullDatasetId(attrName: ExpandedTerm, attrValue: List<Any>): Boolean =
    attrName == NGSILD_DATASET_ID_IRI &&
        (attrValue.firstOrNull() as? Map<*, *>)?.get(JSONLD_ID_KW) == NGSILD_NULL

private fun isNgsiLdNullValue(attrValue: List<Any>): Boolean {
    val value = attrValue.firstOrNull() as? Map<*, *> ?: return false
    return value[JSONLD_VALUE_KW] == NGSILD_NULL || value.isNgsiLdNullJsonLdList()
}

private fun mergeLanguageProperty(
    source: ExpandedAttributeInstance,
    updateLanguageMap: ExpandedLanguageMapValue
): List<Any> {
    val sourceLangEntries = source[NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP] as ExpandedLanguageMapValue
    val targetLangEntries = sourceLangEntries.toMutableList()
    updateLanguageMap.forEach { langEntry ->
        targetLangEntries.removeIf { it[JSONLD_LANGUAGE_KW] == langEntry[JSONLD_LANGUAGE_KW] }
        if (langEntry[JSONLD_VALUE_KW] != NGSILD_NULL)
            targetLangEntries.add(langEntry)
    }
    return targetLangEntries
}

private fun mergeJsonProperty(
    sourceInstance: ExpandedAttributeValue,
    updateInstance: ExpandedAttributeValue
): ExpandedAttributeValue {
    val sourceContent = extractMergeableJsonMap(sourceInstance[0][JSONLD_VALUE_KW])
    val updateContent = extractMergeableJsonMap(updateInstance[0][JSONLD_VALUE_KW])
    if (sourceContent == null || updateContent == null)
        return updateInstance

    val mergedContent = JsonMerger().merge(
        JsonUtils.serializeObject(sourceContent),
        JsonUtils.serializeObject(updateContent)
    ).deserializeAsMap()

    val nullKeys = updateContent.filter { (_, v) -> v == NGSILD_NULL }.keys
    val filteredContent = if (nullKeys.isEmpty()) mergedContent else mergedContent.filterKeys { it !in nullKeys }
    // restore the shape (bare object vs. one-element array) that the update's `@value` had before merging
    val rewrappedContent =
        if (updateInstance[0][JSONLD_VALUE_KW] is List<*>)
            listOf(filteredContent)
        else filteredContent

    return listOf(updateInstance[0] + (JSONLD_VALUE_KW to rewrappedContent))
}

private fun extractMergeableJsonMap(value: Any?): Map<String, Any>? =
    when (value) {
        is Map<*, *> -> value as Map<String, Any>
        is List<*> if value.size == 1 && value[0] is Map<*, *> -> value[0] as Map<String, Any>
        else -> null
    }

private fun mergePropertyOrVocabProperty(
    attrName: String,
    sourceInstance: ExpandedAttributeValue,
    updateInstance: ExpandedAttributeValue
): ExpandedAttributeValue {
    if (updateInstance.size > 1)
        return updateInstance

    val mergedElement = JsonMerger().merge(
        JsonUtils.serializeObject(sourceInstance[0]),
        JsonUtils.serializeObject(updateInstance[0])
    ).deserializeAsMap()

    return listOf(
        if (attrName == NGSILD_PROPERTY_VALUE)
            applyPropertyNgsiLdNullRemoval(mergedElement, updateInstance[0] as ExpandedAttributeInstance)
        else
            mergedElement
    )
}

/**
 * Removes from the merged map any top-level keys that hold an NGSI-LD Null value in the update.
 *
 * Covers Properties with an object value: expanded as nested JSON-LD with IRI keys — null keys are top-level
 * entries whose single expanded value is `[{"@value": "urn:ngsi-ld:null"}]`
 */
private fun applyPropertyNgsiLdNullRemoval(
    merged: Map<String, Any>,
    update: Map<String, Any>
): Map<String, Any> {
    val nullKeys = update.filter { (_, v) ->
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
    when (attributeType) {
        AttributeType.Relationship -> {
            val value = expandedAttributeInstance.getRelationshipId()
            value is URI && value.toString() == NGSILD_NULL
        }
        AttributeType.ListProperty,
        AttributeType.ListRelationship ->
            expandedAttributeInstance
                .getMemberValue(attributeType.toExpandedValueMember())
                .getOrNull()
                .isNgsiLdNullJsonLdList()
        else -> {
            val value = expandedAttributeInstance
                .getMemberValue(attributeType.toExpandedValueMember()).getOrNull()
            value is String && value == NGSILD_NULL
        }
    }

// ["urn:ngsi-ld:null"] is expanded into {"@list": [{"@value": "urn:ngsi-ld:null"}]}
private fun Any?.isNgsiLdNullJsonLdList(): Boolean =
    this == mapOf(JSONLD_LIST_KW to listOf(mapOf(JSONLD_VALUE_KW to NGSILD_NULL)))

fun Json.toExpandedAttributeInstance(): ExpandedAttributeInstance =
    this.deserializeAsMap() as ExpandedAttributeInstance

fun partialUpdatePatch(
    source: ExpandedAttributeInstance,
    update: ExpandedAttributeInstance
): Either<APIException, Pair<String, ExpandedAttributeInstance>> = either {
    val target = source.toMutableMap()
    update.forEach { (attrName, attrValue) ->
        when {
            isNgsiLdNullDatasetId(attrName, attrValue) ->
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
            isNgsiLdNullDatasetId(attrName, attrValue) ->
                raise(BadRequestDataException(NGSI_LD_NULL_NOT_ALLOWED_IN_DATASET_ID_MESSAGE))
            attrName == NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP ->
                target[attrName] = mergeLanguageProperty(source, attrValue as ExpandedLanguageMapValue)
            isNgsiLdNullSubAttribute(attrValue) || isNgsiLdNullValue(attrValue) ->
                target.remove(attrName)
            !source.containsKey(attrName) ->
                target[attrName] = attrValue
            attrName == NGSILD_JSONPROPERTY_JSON -> {
                target[attrName] = mergeJsonProperty(
                    source[attrName]!! as ExpandedAttributeValue,
                    attrValue as ExpandedAttributeValue
                )
            }
            attrName == NGSILD_VOCABPROPERTY_VOCAB || attrName == NGSILD_PROPERTY_VALUE -> {
                target[attrName] = mergePropertyOrVocabProperty(
                    attrName,
                    source[attrName]!! as ExpandedAttributeValue,
                    attrValue as ExpandedAttributeValue
                )
            }
            else -> target[attrName] = attrValue
        }
    }
    Pair(JsonUtils.serializeObject(target), target)
}
