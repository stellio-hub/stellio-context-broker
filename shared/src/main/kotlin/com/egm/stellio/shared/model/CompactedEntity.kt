package com.egm.stellio.shared.model

import com.egm.stellio.shared.model.AttributeCompactedType.GEOPROPERTY
import com.egm.stellio.shared.model.AttributeCompactedType.JSONPROPERTY
import com.egm.stellio.shared.model.AttributeCompactedType.LANGUAGEPROPERTY
import com.egm.stellio.shared.model.AttributeCompactedType.PROPERTY
import com.egm.stellio.shared.model.AttributeCompactedType.RELATIONSHIP
import com.egm.stellio.shared.model.AttributeCompactedType.VOCABPROPERTY
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.FEATURES_PROPERTY_TERM
import com.egm.stellio.shared.util.FEATURE_COLLECTION_TYPE
import com.egm.stellio.shared.util.FEATURE_TYPE
import com.egm.stellio.shared.util.GEOMETRY_PROPERTY_TERM
import com.egm.stellio.shared.util.PROPERTIES_PROPERTY_TERM
import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.util.*

typealias CompactedEntity = Map<String, Any>
typealias CompactedAttributeInstance = Map<String, Any>
typealias CompactedAttributeInstances = List<CompactedAttributeInstance>

val JSONLD_COMPACTED_ATTRIBUTE_CORE_MEMBERS =
    setOf(
        NGSILD_TYPE_TERM,
        NGSILD_VALUE_TERM,
        NGSILD_OBJECT_TERM,
        NGSILD_JSON_TERM,
        NGSILD_VOCAB_TERM,
        NGSILD_LANGUAGEMAP_TERM,
        NGSILD_UNIT_CODE_TERM,
        NGSILD_DATASET_ID_TERM,
        NGSILD_CREATED_AT_TERM,
        NGSILD_MODIFIED_AT_TERM,
        NGSILD_OBSERVED_AT_TERM
    )

fun CompactedEntity.getRelationshipsObjects(): Set<URI> =
    this.mapValues { entry ->
        applyAttributeTransformation(
            entry,
            { value ->
                if (value[NGSILD_TYPE_TERM] == NGSILD_RELATIONSHIP_TERM)
                    setOf(value[NGSILD_OBJECT_TERM] as String)
                else emptySet()
            },
            { values ->
                values.mapNotNull {
                    if (it[NGSILD_TYPE_TERM] == NGSILD_RELATIONSHIP_TERM)
                        it[NGSILD_OBJECT_TERM] as String
                    else null
                }.toSet()
            }
        )
    }.values
        .mapNotNull { it as? Set<String> }
        .fold(emptySet()) { acc, value -> acc.plus(value.map { it.toUri() }) }

fun List<CompactedEntity>.getRelationshipsObjects(): Set<URI> =
    this.map { it.getRelationshipsObjects() }.fold(emptySet()) { acc, value -> acc.plus(value) }

private fun CompactedAttributeInstance.applyInlineLinkedEntity(
    linkedEntities: Map<String, CompactedEntity>
): CompactedAttributeInstance =
    if (
        this[NGSILD_TYPE_TERM] == NGSILD_RELATIONSHIP_TERM &&
        linkedEntities.contains(this[NGSILD_OBJECT_TERM] as String)
    )
        this.plus(NGSILD_ENTITY_TERM to linkedEntities[this[NGSILD_OBJECT_TERM] as String]!!)
    else this

fun CompactedEntity.inlineLinkedEntities(linkedEntities: Map<String, CompactedEntity>): CompactedEntity =
    this.mapValues { entry ->
        applyAttributeTransformation(
            entry,
            { value -> value.applyInlineLinkedEntity(linkedEntities) },
            { values -> values.map { it.applyInlineLinkedEntity(linkedEntities) } }
        )
    }

fun List<CompactedEntity>.inlineLinkedEntities(linkedEntities: Map<String, CompactedEntity>): List<CompactedEntity> =
    this.map { it.inlineLinkedEntities(linkedEntities) }

fun CompactedEntity.filterPickAndOmit(pick: Set<String>, omit: Set<String>): CompactedEntity =
    this.filterKeys {
        pick.isEmpty() || pick.plus(JSONLD_CONTEXT_KW).contains(it)
    }.filterKeys {
        !omit.contains(it)
    }

fun List<CompactedEntity>.filterPickAndOmit(pick: Set<String>, omit: Set<String>): List<CompactedEntity> =
    this.map { it.filterPickAndOmit(pick, omit) }

fun CompactedEntity.toSimplifiedAttributes(): Map<String, Any> =
    this.mapValues { entry ->
        applyAttributeTransformation(entry, ::simplifyAttribute, ::simplifyMultiInstanceAttribute)
    }

private fun simplifyMultiInstanceAttribute(
    value: List<Map<String, Any>>
): Map<String, Map<String, Any>> {
    val datasetIds = value.map {
        val datasetId = it[NGSILD_DATASET_ID_TERM] as? String ?: JSONLD_NONE_KW
        val datasetValue: Any = simplifyAttribute(it)
        Pair(datasetId, datasetValue)
    }
    return mapOf(NGSILD_DATASET_TERM to datasetIds.toMap())
}

private fun simplifyAttribute(value: Map<String, Any>): Any {
    val attributeCompactedType = AttributeCompactedType.forKey(value[NGSILD_TYPE_TERM] as String)!!
    return when (attributeCompactedType) {
        PROPERTY, GEOPROPERTY -> value.getOrDefault(NGSILD_VALUE_TERM, value)
        RELATIONSHIP -> {
            if (value.containsKey(NGSILD_ENTITY_TERM))
                (value[NGSILD_ENTITY_TERM] as CompactedEntity).toSimplifiedAttributes()
            else value.getOrDefault(NGSILD_OBJECT_TERM, value)
        }
        JSONPROPERTY -> mapOf(NGSILD_JSON_TERM to value.getOrDefault(NGSILD_JSON_TERM, value))
        LANGUAGEPROPERTY -> mapOf(NGSILD_LANGUAGEMAP_TERM to value.getOrDefault(NGSILD_LANGUAGEMAP_TERM, value))
        VOCABPROPERTY -> mapOf(NGSILD_VOCAB_TERM to value.getOrDefault(NGSILD_VOCAB_TERM, value))
    }
}

fun CompactedAttributeInstance.getTypeAndValue(): Pair<String, Any?> {
    val attributeCompactedType = AttributeCompactedType.forKey(this[NGSILD_TYPE_TERM] as String)!!
    return when (attributeCompactedType) {
        PROPERTY -> Pair(NGSILD_PROPERTY_TERM, this[NGSILD_VALUE_TERM])
        GEOPROPERTY -> Pair(NGSILD_GEOPROPERTY_TERM, this[NGSILD_VALUE_TERM])
        RELATIONSHIP -> Pair(NGSILD_RELATIONSHIP_TERM, this[NGSILD_OBJECT_TERM])
        JSONPROPERTY -> Pair(NGSILD_JSONPROPERTY_TERM, this[NGSILD_JSON_TERM])
        LANGUAGEPROPERTY -> Pair(NGSILD_LANGUAGEPROPERTY_TERM, this[NGSILD_LANGUAGEMAP_TERM])
        VOCABPROPERTY -> Pair(NGSILD_VOCABPROPERTY_TERM, this[NGSILD_VOCAB_TERM])
    }
}

fun CompactedEntity.toFilteredLanguageProperties(languageFilter: String): CompactedEntity {
    val transformationParameters = mapOf(QueryParameter.LANG.key to languageFilter)
    return this.mapValues { entry ->
        applyAttributeTransformation(
            entry,
            { filterLanguageProperty(it, transformationParameters) },
            { filterMultiInstanceLanguageProperty(it, transformationParameters) },
        )
    }
}

private fun filterMultiInstanceLanguageProperty(
    value: List<Map<String, Any>>,
    transformationParameters: Map<String, String>?
): Any =
    value.map {
        filterLanguageProperty(it, transformationParameters)
    }

private fun filterLanguageProperty(value: Map<String, Any>, transformationParameters: Map<String, String>?): Any {
    val languageFilter = transformationParameters?.get(QueryParameter.LANG.key)!!
    val attributeCompactedType = value[NGSILD_TYPE_TERM]?.let {
        AttributeCompactedType.forKey(value[NGSILD_TYPE_TERM] as String)
    }

    return if (attributeCompactedType == LANGUAGEPROPERTY) {
        val localeRanges = Locale.LanguageRange.parse(languageFilter)
        val propertyLocales = (value[NGSILD_LANGUAGEMAP_TERM] as Map<String, Any>).keys.sorted()
        val bestLocaleMatch = Locale.filterTags(localeRanges, propertyLocales)
            .getOrElse(0) { _ ->
                // as the list is sorted, @none is the first in the list if it exists
                propertyLocales.first()
            }

        value.map { entry ->
            when {
                entry.key == NGSILD_TYPE_TERM ->
                    NGSILD_TYPE_TERM to NGSILD_PROPERTY_TERM
                entry.key == NGSILD_LANGUAGEMAP_TERM ->
                    NGSILD_VALUE_TERM to (value[NGSILD_LANGUAGEMAP_TERM] as Map<String, Any>)[bestLocaleMatch]
                JSONLD_COMPACTED_ATTRIBUTE_CORE_MEMBERS.contains(entry.key) ->
                    entry.key to entry.value
                else ->
                    entry.key to filterLanguageProperty(entry.value as Map<String, Any>, transformationParameters)
            }
        }.toMap()
            .plus(NGSILD_LANG_TERM to bestLocaleMatch)
    } else value.map { entry ->
        when {
            entry.key == NGSILD_ENTITY_TERM ->
                entry.key to (entry.value as CompactedEntity).toFilteredLanguageProperties(languageFilter)
            !JSONLD_COMPACTED_ATTRIBUTE_CORE_MEMBERS.contains(entry.key) ->
                entry.key to filterLanguageProperty(entry.value as Map<String, Any>, transformationParameters)
            else -> entry.key to entry.value
        }
    }.toMap()
}

fun CompactedEntity.toGeoJson(geometryProperty: String): Map<String, Any?> {
    val geometryAttributeContent = this[geometryProperty] as? Map<String, Any>
    val geometryPropertyValue = geometryAttributeContent?.let {
        if (it.containsKey(NGSILD_VALUE_TERM)) it[NGSILD_VALUE_TERM]
        else it
    }

    return mapOf(
        NGSILD_ID_TERM to this[NGSILD_ID_TERM]!!,
        NGSILD_TYPE_TERM to FEATURE_TYPE,
        GEOMETRY_PROPERTY_TERM to geometryPropertyValue,
        PROPERTIES_PROPERTY_TERM to this.filter {
            it.key != NGSILD_ID_TERM && it.key != JSONLD_CONTEXT_KW
        },
        JSONLD_CONTEXT_KW to this[JSONLD_CONTEXT_KW]!!
    )
}

fun CompactedEntity.withoutSysAttrs(sysAttrToKeep: String?): Map<String, Any> {
    val sysAttrsToRemove = NGSILD_SYSATTRS_TERMS.minus(sysAttrToKeep)
    val removeSysAttrsFromAttrInstance = { attrValue: Map<*, *> ->
        attrValue.minus(sysAttrsToRemove)
            .mapValues { entry ->
                if (entry.key == NGSILD_ENTITY_TERM)
                    (entry.value as CompactedEntity).withoutSysAttrs(sysAttrToKeep)
                else entry.value
            }
    }

    return this.filter {
        // deletedAt has to be kept at entity level (but not in attributes), see 5.8.6
        !sysAttrsToRemove.minus(NGSILD_DELETED_AT_TERM).contains(it.key)
    }.mapValues {
        when (it.value) {
            is Map<*, *> -> removeSysAttrsFromAttrInstance(it.value as Map<*, *>)
            is List<*> -> (it.value as List<*>).map { valueInstance ->
                if (valueInstance is Map<*, *>)
                    removeSysAttrsFromAttrInstance(valueInstance)
                // we keep @context value as it is (List<String>)
                else valueInstance
            }
            else -> it.value
        }
    }
}

/**
 * Create the final representation of the entity, taking into account the options parameter and the Accept header.
 *
 * As a GeoJSON representation may have a null value for a key, returns a Map<String, Any?> instead of CompactedEntity.
 */
fun CompactedEntity.toFinalRepresentation(
    ngsiLdDataRepresentation: NgsiLdDataRepresentation
): Map<String, Any?> =
    this.let {
        if (!ngsiLdDataRepresentation.includeSysAttrs) it.withoutSysAttrs(ngsiLdDataRepresentation.timeproperty)
        else it
    }.let {
        if (ngsiLdDataRepresentation.languageFilter != null)
            it.toFilteredLanguageProperties(ngsiLdDataRepresentation.languageFilter)
        else it
    }.let {
        if (ngsiLdDataRepresentation.attributeRepresentation == AttributeRepresentation.SIMPLIFIED)
            it.toSimplifiedAttributes()
        else it
    }.let {
        when (ngsiLdDataRepresentation.entityRepresentation) {
            EntityRepresentation.GEO_JSON ->
                // geometryProperty is not null when GeoJSON representation is asked (defaults to location)
                it.toGeoJson(ngsiLdDataRepresentation.geometryProperty!!)
            EntityRepresentation.JSON -> it.minus(JSONLD_CONTEXT_KW)
            EntityRepresentation.JSON_LD -> it
        }
    }

/**
 * Create the final representation of a list of entities, taking into account the options parameter
 * and the Accept header.
 *
 * For a GeoJSON representation, the result is a map containing a list of GeoJson objects.
 */
fun List<CompactedEntity>.toFinalRepresentation(
    ngsiLdDataRepresentation: NgsiLdDataRepresentation
): Any =
    this.map {
        it.toFinalRepresentation(ngsiLdDataRepresentation)
    }.let {
        if (ngsiLdDataRepresentation.entityRepresentation == EntityRepresentation.GEO_JSON) {
            mapOf(
                NGSILD_TYPE_TERM to FEATURE_COLLECTION_TYPE,
                FEATURES_PROPERTY_TERM to it
            )
        } else it
    }

enum class AttributeCompactedType(val key: String) {
    PROPERTY(NGSILD_PROPERTY_TERM),
    RELATIONSHIP(NGSILD_RELATIONSHIP_TERM),
    GEOPROPERTY(NGSILD_GEOPROPERTY_TERM),
    JSONPROPERTY(NGSILD_JSONPROPERTY_TERM),
    LANGUAGEPROPERTY(NGSILD_LANGUAGEPROPERTY_TERM),
    VOCABPROPERTY(NGSILD_VOCABPROPERTY_TERM);

    companion object {
        fun forKey(key: String): AttributeCompactedType? =
            entries.find { it.key == key }
    }
}

fun applyAttributeTransformation(
    entry: Map.Entry<String, Any>,
    onSingleInstance: (Map<String, Any>) -> Any,
    onMultiInstance: (List<Map<String, Any>>) -> Any
): Any =
    when {
        COMPACTED_ENTITY_CORE_MEMBERS.contains(entry.key) -> entry.value
        // an attribute with a single instance
        entry.value is Map<*, *> -> onSingleInstance(entry.value as Map<String, Any>)
        // an attribute with multiple instances
        entry.value is List<*> -> onMultiInstance(entry.value as List<Map<String, Any>>)
        // should not happen... just return the value
        else -> entry.value
    }
