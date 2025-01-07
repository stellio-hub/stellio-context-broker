package com.egm.stellio.shared.queryparameter

enum class FormatValue(val value: String) {
    KEY_VALUES("keyValues"),
    SIMPLIFIED("simplified"),
    NORMALIZED("normalized"),
    TEMPORAL_VALUES("temporalValues"),
    AGGREGATED_VALUES("aggregatedValues");
    companion object {
        fun fromString(key: String): FormatValue? =
            FormatValue.entries.find { it.value == key }
    }
}
