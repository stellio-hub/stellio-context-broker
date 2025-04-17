package com.egm.stellio.search.authorization.permission.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.fasterxml.jackson.annotation.JsonProperty

enum class Action(val value: String, private val includedIn: Set<Action> = emptySet()) {
    @JsonProperty("own")
    OWN("own"),

    @JsonProperty("admin")
    ADMIN("admin", includedIn = setOf(OWN)),

    @JsonProperty("write")
    WRITE("write", includedIn = setOf(OWN, ADMIN)),

    @JsonProperty("read")
    READ("read", includedIn = setOf(OWN, ADMIN, WRITE));

    fun getIncludedIn(): Set<Action> = includedIn + this

    companion object {
        fun fromString(action: String): Either<APIException, Action> =
            Action.entries.find { it.value == action }?.right()
                ?: BadRequestDataException("invalid action provided").left()
    }
}
