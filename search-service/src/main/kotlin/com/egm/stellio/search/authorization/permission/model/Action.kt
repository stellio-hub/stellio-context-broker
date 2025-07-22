package com.egm.stellio.search.authorization.permission.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.util.AuthContextModel
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
                ?: BadRequestDataException(
                    """Invalid action provided: "$action", must be "own", "admin","write" or "read"."""
                ).left()
    }
}

fun NgsiLdEntity.getSpecificAccessPolicy(): Either<APIException, Action>? =
    this.properties.find { it.name == AuthContextModel.AUTH_PROP_SAP }?.getSpecificAccessPolicy()

fun NgsiLdAttribute.getSpecificAccessPolicy(): Either<APIException, Action> {
    val ngsiLdAttributeInstances = this.getAttributeInstances()
    if (ngsiLdAttributeInstances.size > 1)
        return BadRequestDataException("Payload must contain a single attribute instance").left()
    val ngsiLdAttributeInstance = ngsiLdAttributeInstances[0]
    if (ngsiLdAttributeInstance !is NgsiLdPropertyInstance)
        return BadRequestDataException("Payload must be a property").left()

    return when (ngsiLdAttributeInstance.value.toString()) {
        "AUTH_READ" -> Action.READ.right()
        "AUTH_WRITE" -> Action.WRITE.right()
        else -> Action.fromString(ngsiLdAttributeInstance.value.toString())
            .onRight {
                if (it !in setOf(Action.READ, Action.WRITE))
                    BadRequestDataException("Only read and write are accepted as global policy").left()
                else it.right()
            }
    }
}
