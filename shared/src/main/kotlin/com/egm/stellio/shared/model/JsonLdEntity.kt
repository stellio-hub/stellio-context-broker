package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.castAttributeValue
import java.time.ZonedDateTime

typealias CompactedJsonLdEntity = Map<String, Any>

data class JsonLdEntity(
    val members: Map<String, Any>,
    val contexts: List<String>
) {
    fun containsAnyOf(expandedAttributes: Set<String>): Boolean =
        expandedAttributes.isEmpty() || members.keys.any { expandedAttributes.contains(it) }

    fun checkContainsAnyOf(expandedAttributes: Set<String>): Either<APIException, Unit> =
        if (containsAnyOf(expandedAttributes))
            Unit.right()
        else ResourceNotFoundException(entityOrAttrsNotFoundMessage(id, expandedAttributes)).left()

    fun getAttributes(): ExpandedAttributes =
        members.filter { !JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it.key) }
            .mapValues { castAttributeValue(it.value) }

    fun getScopes(): List<String>? =
        (members as Map<String, List<Any>>).getScopes()

    /**
     * Called at entity creation time to populate entity and attributes with createdAt information
     */
    fun populateCreationTimeDate(createdAt: ZonedDateTime): JsonLdEntity =
        JsonLdEntity(
            members = members.mapValues {
                if (JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it.key))
                    it.value
                else castAttributeValue(it.value).map { expandedAttributeInstance ->
                    expandedAttributeInstance.addDateTimeProperty(
                        NGSILD_CREATED_AT_PROPERTY,
                        createdAt
                    ) as ExpandedAttributeInstance
                }
            }.addDateTimeProperty(NGSILD_CREATED_AT_PROPERTY, createdAt),
            contexts = contexts
        )

    /**
     * Called when replacing entity to populate entity and attributes with createdAt and modifiedAt information
     * for attributes, the modification date is added as the creation date
     */
    fun populateReplacementTimeDates(createdAt: ZonedDateTime, replacedAt: ZonedDateTime): JsonLdEntity =
        JsonLdEntity(
            members = members.mapValues {
                if (JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it.key))
                    it.value
                else castAttributeValue(it.value).map { expandedAttributeInstance ->
                    expandedAttributeInstance.addDateTimeProperty(
                        NGSILD_CREATED_AT_PROPERTY,
                        replacedAt
                    ) as ExpandedAttributeInstance
                }
            }
                .addDateTimeProperty(NGSILD_CREATED_AT_PROPERTY, createdAt)
                .addDateTimeProperty(NGSILD_MODIFIED_AT_PROPERTY, replacedAt),
            contexts = contexts
        )

    // FIXME kinda hacky but we often just need the id or type... how can it be improved?
    val id by lazy {
        (members[JSONLD_ID] ?: InternalErrorException("Could not extract id from JSON-LD entity")) as String
    }

    val types by lazy {
        (members[JSONLD_TYPE] ?: InternalErrorException("Could not extract type from JSON-LD entity"))
            as List<ExpandedTerm>
    }

    private fun Map<String, Any>.addDateTimeProperty(propertyKey: String, dateTime: ZonedDateTime?): Map<String, Any> =
        if (dateTime != null)
            this.plus(propertyKey to JsonLdUtils.buildNonReifiedTemporalValue(dateTime))
        else this
}

fun CompactedJsonLdEntity.toKeyValues(): Map<String, Any> =
    this.mapValues { (_, value) -> simplifyRepresentation(value) }

private fun simplifyRepresentation(value: Any): Any {
    return when (value) {
        // entity property value is always a Map
        is Map<*, *> -> simplifyValue(value as Map<String, Any>)
        is List<*> -> value.map {
            when (it) {
                is Map<*, *> -> simplifyValue(it as Map<String, Any>)
                // we keep @context value as it is (List<String>)
                else -> it
            }
        }
        // we keep id and type values as they are (String)
        else -> value
    }
}

private fun simplifyValue(value: Map<String, Any>): Any {
    return when (value["type"]) {
        "Property", "GeoProperty" -> value.getOrDefault("value", value)
        "Relationship" -> value.getOrDefault("object", value)
        else -> value
    }
}

fun CompactedJsonLdEntity.toGeoJson(geometryProperty: String): Map<String, Any?> {
    val geometryAttributeContent = this[geometryProperty] as? Map<String, Any>
    val geometryPropertyValue = geometryAttributeContent?.let {
        if (it.containsKey(JSONLD_VALUE_TERM)) it[JSONLD_VALUE_TERM]
        else it
    }

    return mapOf(
        JSONLD_ID_TERM to this[JSONLD_ID_TERM]!!,
        JSONLD_TYPE_TERM to FEATURE_TYPE,
        GEOMETRY_PROPERTY_TERM to geometryPropertyValue,
        PROPERTIES_PROPERTY_TERM to this.filter { it.key != JSONLD_ID_TERM }
    )
}

fun CompactedJsonLdEntity.withoutSysAttrs(): Map<String, Any> =
    this.filter {
        !JsonLdUtils.NGSILD_SYSATTRS_TERMS.contains(it.key)
    }.mapValues {
        when (it.value) {
            is Map<*, *> -> (it.value as Map<*, *>).minus(JsonLdUtils.NGSILD_SYSATTRS_TERMS)
            is List<*> -> (it.value as List<*>).map { valueInstance ->
                when (valueInstance) {
                    is Map<*, *> -> valueInstance.minus(JsonLdUtils.NGSILD_SYSATTRS_TERMS)
                    // we keep @context value as it is (List<String>)
                    else -> valueInstance
                }
            }
            else -> it.value
        }
    }

fun CompactedJsonLdEntity.toFinalRepresentation(
    ngsiLdDataRepresentation: NgsiLdDataRepresentation
): Any =
    this.let {
        if (!ngsiLdDataRepresentation.includeSysAttrs) it.withoutSysAttrs()
        else it
    }.let {
        if (ngsiLdDataRepresentation.attributeRepresentation == AttributeRepresentation.SIMPLIFIED) it.toKeyValues()
        else it
    }.let {
        if (ngsiLdDataRepresentation.entityRepresentation == EntityRepresentation.GEO_JSON)
            // geometryProperty is not null when GeoJSON representation is asked (defaults to location)
            it.toGeoJson(ngsiLdDataRepresentation.geometryProperty!!)
        else it
    }

fun List<CompactedJsonLdEntity>.toFinalRepresentation(
    ngsiLdDataRepresentation: NgsiLdDataRepresentation
): Any =
    this.map {
        it.toFinalRepresentation(ngsiLdDataRepresentation)
    }.let {
        if (ngsiLdDataRepresentation.entityRepresentation == EntityRepresentation.GEO_JSON) {
            mapOf(
                JSONLD_TYPE_TERM to FEATURE_COLLECTION_TYPE,
                FEATURES_PROPERTY_TERM to it
            )
        } else it
    }
