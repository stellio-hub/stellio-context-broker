package com.egm.stellio.search.util

import com.egm.stellio.shared.util.JsonUtils
import java.net.URI
import java.time.ZonedDateTime

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

fun buildAttributeInstancePayload(
    value: Any,
    observedAt: ZonedDateTime,
    datasetId: URI? = null,
    instanceId: URI? = null
): String {
    val payload = mutableMapOf<String, Any>("type" to "Property")
    datasetId?.let { payload["datasetId"] = it }
    payload["value"] = value
    instanceId?.let { payload["instanceId"] = it }
    payload["observedAt"] = observedAt
    return JsonUtils.serializeObject(payload)
}
