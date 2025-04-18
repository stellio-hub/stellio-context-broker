package com.egm.stellio.search.authorization.permission.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.fasterxml.jackson.annotation.JsonProperty

enum class Action(val value: String) {
    @JsonProperty("read")
    READ("read"),

    @JsonProperty("write")
    WRITE("write"),

    @JsonProperty("admin")
    ADMIN("admin");

    companion object {
        fun fromString(action: String): Either<APIException, Action> =
            Action.entries.find { it.value == action }?.right()
                ?: BadRequestDataException("invalid action provided").left()
    }
}
