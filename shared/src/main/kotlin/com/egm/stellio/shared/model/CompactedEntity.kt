package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SYSATTRS_TERMS

typealias CompactedEntity = Map<String, Any>

fun CompactedEntity.toKeyValues(): Map<String, Any> =
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
    return when (value[JSONLD_TYPE_TERM]) {
        NGSILD_PROPERTY_TERM, NGSILD_GEOPROPERTY_TERM -> value.getOrDefault(JSONLD_VALUE_TERM, value)
        NGSILD_RELATIONSHIP_TERM -> value.getOrDefault(JSONLD_OBJECT, value)
        else -> value
    }
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

fun CompactedEntity.toFinalRepresentation(
    ngsiLdDataRepresentation: NgsiLdDataRepresentation
): Any =
    this.let {
        if (!ngsiLdDataRepresentation.includeSysAttrs) it.withoutSysAttrs(ngsiLdDataRepresentation.timeproperty)
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
