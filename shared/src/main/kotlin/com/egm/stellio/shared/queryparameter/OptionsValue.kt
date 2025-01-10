package com.egm.stellio.shared.queryparameter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.InvalidRequestException

enum class OptionsValue(val value: String) {
    SYS_ATTRS("sysAttrs"),
    NO_OVERWRITE("noOverwrite"),
    UPDATE_MODE("update"),
    REPLACE_MODE("replace"),
    TEMPORAL_VALUES("temporalValues"),
    AGGREGATED_VALUES("aggregatedValues"),
    AUDIT("audit"),
    NORMALIZED("normalized"),
    KEY_VALUES("keyValues"),
    SIMPLIFIED("simplified");
    companion object {
        fun fromString(key: String): Either<APIException, OptionsValue> = either {
            OptionsValue.entries.find { it.value == key }
                ?: return InvalidRequestException("'$key' is not a valid value for the options query parameter").left()
        }
    }
}
