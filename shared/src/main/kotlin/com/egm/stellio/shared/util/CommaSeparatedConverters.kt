package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.ErrorMessages.Json.notAStringOrArrayOfStringsMessage
import tools.jackson.databind.util.StdConverter

/**
 * Reads a "comma separated list" member (e.g. jsonKeys, expandValues in 5.2.12 and 5.2.23) from a String, as defined
 * in the spec, or from a JSON array of Strings (kept for backward compatibility).
 * An empty String is considered as an absent member. The order of the values is preserved.
 *
 * An invalid input raises an exception, that Jackson wraps into a parsing error of the payload.
 */
class CommaSeparatedToSetConverter : StdConverter<Any, Set<String>?>() {
    override fun convert(value: Any): Set<String>? =
        when (value) {
            is String -> value.split(",").map { it.trim() }.filter { it.isNotEmpty() }
            is List<*> -> value.map { it as? String ?: invalidValue(value) }
            else -> invalidValue(value)
        }.toSet().ifEmpty { null }

    private fun invalidValue(value: Any): Nothing =
        throw IllegalArgumentException(notAStringOrArrayOfStringsMessage(value))
}

class SetToCommaSeparatedConverter : StdConverter<Set<String>, String>() {
    override fun convert(value: Set<String>): String = value.joinToString(",")
}
