package com.egm.stellio.shared.queryparameter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.InvalidRequestException

enum class FormatValue(val value: String) {
    KEY_VALUES("keyValues"),
    SIMPLIFIED("simplified"),
    NORMALIZED("normalized"),
    TEMPORAL_VALUES("temporalValues"),
    AGGREGATED_VALUES("aggregatedValues");
    companion object {
        fun fromString(key: String): Either<APIException, FormatValue> = either {
            FormatValue.entries.find { it.value == key }
                ?: return InvalidRequestException("'$key' is not a valid value for the format query parameter").left()
        }
    }
}
