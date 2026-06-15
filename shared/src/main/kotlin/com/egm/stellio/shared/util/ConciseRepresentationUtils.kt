package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.JSONLD_COMPACTED_ATTRIBUTE_CORE_MEMBERS
import com.egm.stellio.shared.model.NGSILD_ATTRIBUTE_TYPES
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_TERM
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_TERM
import com.egm.stellio.shared.model.NGSILD_JSON_TERM
import com.egm.stellio.shared.model.NGSILD_LANGUAGEMAP_TERM
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_TERM
import com.egm.stellio.shared.model.NGSILD_OBJECT_TERM
import com.egm.stellio.shared.model.NGSILD_PROPERTY_TERM
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_TERM
import com.egm.stellio.shared.model.NGSILD_VOCAB_TERM
import com.egm.stellio.shared.queryparameter.GeoQuery.GeometryType

fun normalizeEntityFragment(payload: Map<String, Any>): Map<String, Any> =
    payload.mapValues { (key, value) ->
        if (COMPACTED_ENTITY_CORE_MEMBERS.contains(key))
            value
        else if (value is List<*>)
            if (value.isEmpty()) value
            else if (value.first() is Map<*, *>)
                value.map { normalizeAttributeFragment(it as Map<String, Any>) }
            else normalizeAttributeFragment(value)
        else normalizeAttributeFragment(value)
    }

fun normalizeAttributeFragment(value: Any): Map<String, Any> =
    when (value) {
        is String, is Number, is Boolean -> mapOf(NGSILD_TYPE_TERM to NGSILD_PROPERTY_TERM, NGSILD_VALUE_TERM to value)
        is Map<*, *> -> {
            val compactedAttributeInstance = value as CompactedAttributeInstance
            val attributeType = compactedAttributeInstance[NGSILD_TYPE_TERM]

            when {
                // If the provided value is a JSON object and contains a "type" field, the whole Attribute shall be
                // treated as a normalized representation (only said in 4.5.2.3 but apply for all types of attributes)
                NGSILD_ATTRIBUTE_TYPES.contains(attributeType) ->
                    compactedAttributeInstance
                isGeoJsonGeometry(compactedAttributeInstance) ->
                    mapOf(NGSILD_TYPE_TERM to NGSILD_GEOPROPERTY_TERM, NGSILD_VALUE_TERM to compactedAttributeInstance)
                compactedAttributeInstance.containsKey(NGSILD_OBJECT_TERM) ->
                    normalizeSubAttrs(compactedAttributeInstance.plus(NGSILD_TYPE_TERM to NGSILD_RELATIONSHIP_TERM))
                compactedAttributeInstance.containsKey(NGSILD_JSON_TERM) ->
                    normalizeSubAttrs(compactedAttributeInstance.plus(NGSILD_TYPE_TERM to NGSILD_JSONPROPERTY_TERM))
                compactedAttributeInstance.containsKey(NGSILD_LANGUAGEMAP_TERM) ->
                    normalizeSubAttrs(compactedAttributeInstance.plus(NGSILD_TYPE_TERM to NGSILD_LANGUAGEPROPERTY_TERM))
                compactedAttributeInstance.containsKey(NGSILD_VOCAB_TERM) ->
                    normalizeSubAttrs(compactedAttributeInstance.plus(NGSILD_TYPE_TERM to NGSILD_VOCABPROPERTY_TERM))
                compactedAttributeInstance.containsKey(NGSILD_VALUE_TERM) &&
                    isGeoJsonGeometry(compactedAttributeInstance[NGSILD_VALUE_TERM]) ->
                    normalizeSubAttrs(compactedAttributeInstance.plus(NGSILD_TYPE_TERM to NGSILD_GEOPROPERTY_TERM))
                compactedAttributeInstance.containsKey(NGSILD_VALUE_TERM) ->
                    normalizeSubAttrs(compactedAttributeInstance.plus(NGSILD_TYPE_TERM to NGSILD_PROPERTY_TERM))
                else ->
                    mapOf(NGSILD_TYPE_TERM to NGSILD_PROPERTY_TERM, NGSILD_VALUE_TERM to compactedAttributeInstance)
            }
        }
        else -> mapOf(NGSILD_TYPE_TERM to NGSILD_PROPERTY_TERM, NGSILD_VALUE_TERM to value)
    }

private fun normalizeSubAttrs(compactedAttributeInstance: CompactedAttributeInstance): Map<String, Any> =
    compactedAttributeInstance.mapValues { (key, value) ->
        if (JSONLD_COMPACTED_ATTRIBUTE_CORE_MEMBERS.contains(key))
            value
        else normalizeSubAttrValue(value)
    }

private fun normalizeSubAttrValue(value: Any): Any =
    if (value is List<*>) value.map { element ->
        if (element is Map<*, *>) normalizeAttributeFragment(element as Map<String, Any>)
        else element
    }
    else normalizeAttributeFragment(value)

private fun isGeoJsonGeometry(value: Any?): Boolean {
    if (value !is Map<*, *>) return false
    val type = value["type"] as? String ?: return false
    return GeometryType.isSupportedType(type) && value.containsKey("coordinates")
}
