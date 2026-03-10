package com.egm.stellio.search.authorization.permission.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.AuthContextModel.AUTHENTICATED_SUBJECT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PERMISSION_TERM
import com.egm.stellio.shared.util.DataTypes.convertTo
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.invalidTypeMessage
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.invalidUriMessage
import com.egm.stellio.shared.util.ErrorMessages.Permission.permissionFailedToParseMessage
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.getSubFromSecurityContext
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

/**
 * Permission inspired by https://www.w3.org/TR/odrl-model/#permission
 */
data class Permission(
    val id: URI = "urn:ngsi-ld:Permission:${UUID.randomUUID()}".toUri(),
    val type: String = AUTH_PERMISSION_TERM,
    val target: TargetAsset, // odrl:target
    val assignee: Sub = AUTHENTICATED_SUBJECT, // odrl:assignee
    val action: Action, // odrl:action
    val createdAt: ZonedDateTime = ngsiLdDateTime(),
    val modifiedAt: ZonedDateTime = createdAt,
    val assigner: Sub? = null // odrl:assigner
) {
    fun expand(contexts: List<String>): Permission =
        this.copy(
            target = target.expand(contexts),
        )

    fun compact(contexts: List<String>): Permission =
        this.copy(
            target = target.compact(contexts),
        )

    fun validate(): Either<APIException, Unit> = either {
        checkTypeIsPermission().bind()
        checkIdIsValid().bind()
        target.validate().bind()
    }

    private fun checkTypeIsPermission(): Either<APIException, Unit> =
        if (type != AUTH_PERMISSION_TERM)
            BadRequestDataException(invalidTypeMessage(AUTH_PERMISSION_TERM)).left()
        else Unit.right()

    private fun checkIdIsValid(): Either<APIException, Unit> =
        if (!id.isAbsolute)
            BadRequestDataException(invalidUriMessage("$id")).left()
        else Unit.right()

    suspend fun mergeWithFragment(
        fragment: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, Permission> = either {
        val mergedPermission = convertTo<Map<String, Any>>(this@Permission).plus(fragment)
        deserialize(mergedPermission, contexts).bind()
            .copy(modifiedAt = ngsiLdDateTime(), assigner = getSubFromSecurityContext())
    }

    companion object {
        fun deserialize(
            input: Map<String, Any>,
            contexts: List<String>
        ): Either<APIException, Permission> =
            runCatching {
                deserializeAs<Permission>(serializeObject(input.plus(JSONLD_CONTEXT_KW to contexts)))
                    .expand(contexts)
            }.fold(
                { it.right() },
                { it.toAPIException(permissionFailedToParseMessage(it.message)).left() }
            )
    }
}
