package com.egm.stellio.search.util

import com.egm.stellio.shared.model.CompactedJsonLdAttribute
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.JsonUtils
import java.net.URI

fun isAttributeOfMeasureType(value: Any): Boolean =
    value is Double || value is Int

fun valueToDoubleOrNull(value: Any): Double? =
    when (value) {
        is Double -> value
        is Int -> value.toDouble()
        else -> null
    }

fun valueToStringOrNull(value: Any): String? =
    when (value) {
        is String -> value
        else -> null
    }

fun extractAttributeInstanceFromParsedPayload(
    parsedPayload: CompactedJsonLdEntity,
    attributeName: String,
    datasetId: URI?,
    instanceId: URI
): String {
    val attributeInstancePayload = if (parsedPayload[attributeName] is List<*>) {
        val attributePayload = parsedPayload[attributeName] as List<CompactedJsonLdAttribute>
        attributePayload.first { it["datasetId"] as String? == datasetId?.toString() }
    } else parsedPayload[attributeName]!! as CompactedJsonLdAttribute

    val enrichedAttributeInstancePayload = attributeInstancePayload.toMutableMap()
    enrichedAttributeInstancePayload["instanceId"] = instanceId.toString()

    return JsonUtils.serializeObject(enrichedAttributeInstancePayload)
}
