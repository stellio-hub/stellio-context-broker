package com.egm.stellio.search.util

import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.extractAttributeInstanceFromParsedPayload
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

fun extractAttributeInstanceAndAddInstanceId(
    parsedPayload: CompactedJsonLdEntity,
    attributeName: String,
    datasetId: URI?,
    instanceId: URI
): Map<String, Any> {
    val attributeInstancePayload = extractAttributeInstanceFromParsedPayload(
        parsedPayload,
        attributeName,
        datasetId
    )
    val enrichedAttributeInstancePayload = attributeInstancePayload.toMutableMap()
    enrichedAttributeInstancePayload["instanceId"] = instanceId.toString()

    return enrichedAttributeInstancePayload.toMap()
}
