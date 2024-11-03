package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.LinkedEntityQuery
import com.egm.stellio.shared.model.LinkedEntityQuery.Companion.DEFAULT_JOIN_LEVEL
import com.egm.stellio.shared.model.LinkedEntityQuery.JoinType

fun parseLinkedEntityQueryParameters(
    join: String?,
    joinLevel: String?,
    containedBy: String?
): Either<APIException, LinkedEntityQuery?> = either {
    val containedBy = containedBy?.split(",").orEmpty().toListOfUri().toSet()
    val join = join?.let {
        if (JoinType.isSupportedType(it))
            JoinType.forType(it).right()
        else
            BadRequestDataException(
                "'$it' is not a recognized value for 'join' parameter (only 'flat', 'inline' and '@none' are allowed)"
            ).left()
    }?.bind()
    val joinLevel = joinLevel?.let { param ->
        runCatching {
            param.toUInt()
        }.fold(
            { it.right() },
            {
                BadRequestDataException(
                    "'$param' is not a recognized value for 'joinLevel' parameter (only positive integers are allowed)"
                ).left()
            }
        )
    }?.bind()

    if ((joinLevel != null || containedBy.isNotEmpty()) && join == null)
        raise(BadRequestDataException("'join' must be specified if 'joinLevel' or 'containedBy' are specified"))
    else join?.let { LinkedEntityQuery(it, joinLevel ?: DEFAULT_JOIN_LEVEL.toUInt(), containedBy) }
}
