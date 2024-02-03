package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_JSON_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SYSATTRS_TERMS

typealias CompactedEntity = Map<String, Any>

fun CompactedEntity.toKeyValues(): Map<String, Any> =
    this.mapValues { (_, value) -> simplifyRepresentation(value) }

private fun simplifyRepresentation(value: Any): Any =
    when (value) {
        // an attribute with a single instance
        is Map<*, *> -> simplifyValue(value as Map<String, Any>)
        // an attribute with multiple instances
        is List<*> -> value.map {
            when (it) {
                is Map<*, *> -> simplifyValue(it as Map<String, Any>)
                // we keep @context value as it is (List<String>)
                else -> it
            }
        }
        // keep id, type and other non-reified properties as they are (typically string or list)
        else -> value
    }

private fun simplifyValue(value: Map<String, Any>): Any =
    when (value[JSONLD_TYPE_TERM]) {
        NGSILD_PROPERTY_TERM, NGSILD_GEOPROPERTY_TERM -> value.getOrDefault(JSONLD_VALUE_TERM, value)
        NGSILD_JSONPROPERTY_TERM -> mapOf(JSONLD_JSON_TERM to value.getOrDefault(JSONLD_JSON_TERM, value))
        NGSILD_RELATIONSHIP_TERM -> value.getOrDefault(JSONLD_OBJECT, value)
        else -> value
    }

fun CompactedEntity.toGeoJson(geometryProperty: String): Map<String, Any?> {
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

fun CompactedEntity.withoutSysAttrs(sysAttrToKeep: String?): Map<String, Any> {
    val sysAttrsToRemove = NGSILD_SYSATTRS_TERMS.minus(sysAttrToKeep)
    return this.filter {
        !sysAttrsToRemove.contains(it.key)
    }.mapValues {
        when (it.value) {
            is Map<*, *> -> (it.value as Map<*, *>).minus(sysAttrsToRemove)
            is List<*> -> (it.value as List<*>).map { valueInstance ->
                when (valueInstance) {
                    is Map<*, *> -> valueInstance.minus(sysAttrsToRemove)
                    // we keep @context value as it is (List<String>)
                    else -> valueInstance
                }
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
        if (ngsiLdDataRepresentation.attributeRepresentation == AttributeRepresentation.SIMPLIFIED) it.toKeyValues()
        else it
    }.let {
        when (ngsiLdDataRepresentation.entityRepresentation) {
            EntityRepresentation.GEO_JSON ->
                // geometryProperty is not null when GeoJSON representation is asked (defaults to location)
                it.toGeoJson(ngsiLdDataRepresentation.geometryProperty!!)
            EntityRepresentation.JSON -> it.minus(JSONLD_CONTEXT)
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
                JSONLD_TYPE_TERM to FEATURE_COLLECTION_TYPE,
                FEATURES_PROPERTY_TERM to it
            )
        } else it
    }
