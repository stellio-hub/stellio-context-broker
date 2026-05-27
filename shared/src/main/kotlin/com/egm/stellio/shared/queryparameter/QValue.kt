package com.egm.stellio.shared.queryparameter

import com.egm.stellio.shared.util.isDate
import com.egm.stellio.shared.util.isDateTime
import com.egm.stellio.shared.util.isTime
import com.egm.stellio.shared.util.isURI

sealed class QValue

data class SingleValue(val raw: String, val type: ValueType) : QValue()
data class RangeValue(val low: SingleValue, val high: SingleValue) : QValue()
data class ListValue(val items: List<SingleValue>) : QValue()

enum class ValueType {
    NUMBER, STRING, BOOLEAN, DATETIME, DATE, TIME, URI;

    companion object {
        fun detect(raw: String): ValueType {
            val unquoted = raw.removeSurrounding("\"")
            return when {
                raw == "true" || raw == "false" -> BOOLEAN
                unquoted.toDoubleOrNull() != null -> NUMBER
                unquoted.isDateTime() -> DATETIME
                unquoted.isDate() -> DATE
                unquoted.isTime() -> TIME
                raw.isURI() || unquoted.isURI() -> URI
                else -> STRING
            }
        }
    }
}
