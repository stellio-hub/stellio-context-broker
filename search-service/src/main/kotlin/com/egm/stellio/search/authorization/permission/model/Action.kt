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
import com.egm.stellio.shared.util.BatchOperationErrorMessages.PAYLOAD_MUST_BE_PROPERTY_MESSAGE
import com.egm.stellio.shared.util.BatchOperationErrorMessages.PAYLOAD_SINGLE_INSTANCE_MESSAGE
import com.egm.stellio.shared.util.PermissionErrorMessages.GLOBAL_POLICY_RESTRICTION_MESSAGE
import com.egm.stellio.shared.util.PermissionErrorMessages.invalidActionMessage
import com.egm.stellio.shared.util.toSqlList
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

    fun includedInToSqlList(): String =
        getIncludedIn().map { it.value }.toSqlList()

    companion object {
        fun fromString(action: String): Either<APIException, Action> =
            Action.entries.find { it.value == action }?.right()
                ?: BadRequestDataException(invalidActionMessage(action)).left()
    }
}

fun NgsiLdEntity.getSpecificAccessPolicy(): Either<APIException, Action>? =
    this.properties.find { it.name == AuthContextModel.AUTH_PROP_SAP }?.getSpecificAccessPolicy()

fun NgsiLdAttribute.getSpecificAccessPolicy(): Either<APIException, Action> {
    val ngsiLdAttributeInstances = this.getAttributeInstances()
    if (ngsiLdAttributeInstances.size > 1)
        return BadRequestDataException(PAYLOAD_SINGLE_INSTANCE_MESSAGE).left()
    val ngsiLdAttributeInstance = ngsiLdAttributeInstances[0]
    if (ngsiLdAttributeInstance !is NgsiLdPropertyInstance)
        return BadRequestDataException(PAYLOAD_MUST_BE_PROPERTY_MESSAGE).left()

    return when (ngsiLdAttributeInstance.value.toString()) {
        "AUTH_READ" -> Action.READ.right()
        "AUTH_WRITE" -> Action.WRITE.right()
        else -> Action.fromString(ngsiLdAttributeInstance.value.toString())
            .onRight {
                if (it !in setOf(Action.READ, Action.WRITE))
                    BadRequestDataException(GLOBAL_POLICY_RESTRICTION_MESSAGE).left()
                else it.right()
            }
    }
}
