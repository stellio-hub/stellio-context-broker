package com.egm.stellio.search.authorization.permission.model

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.shared.util.toListOfUri
import org.springframework.util.MultiValueMap
import java.net.URI

open class PermissionFilters(
    val ids: Set<URI>? = null,
    val action: Action? = null,
    val assignee: Sub? = null,
    val assigner: Sub? = null,
    val targetTypeSelection: String? = null,
    val kind: PermissionKind = PermissionKind.ADMIN,
) {

    companion object {
        enum class PermissionKind(val value: String) {
            ADMIN("admin"),
            ASSIGNED("assigned")
        }

        fun fromQueryParameters(
            queryParams: MultiValueMap<String, String>,
            contexts: List<String>,
            kind: PermissionKind = PermissionKind.ADMIN
        ): Either<APIException, PermissionFilters> = either {
            val ids = queryParams.getFirst(QueryParameter.TARGET_ID.key)?.split(",").orEmpty().toListOfUri().toSet()

            val action = queryParams.getFirst(QueryParameter.ACTION.key)?.let { Action.fromString(it).bind() }
            val assignee = queryParams.getFirst(QueryParameter.ASSIGNEE.key)
            val assigner = queryParams.getFirst(QueryParameter.ASSIGNER.key)
            val targetType = expandTypeSelection(queryParams.getFirst(QueryParameter.TARGET_TYPE.key), contexts)

            PermissionFilters(ids, action, assignee, assigner, targetType, kind)
        }
    }
}
