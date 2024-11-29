package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.util.toListOfUri
import java.net.URI
import kotlin.UInt

data class LinkedEntityQuery(
    val join: JoinType = JoinType.NONE,
    val joinLevel: UInt = DEFAULT_JOIN_LEVEL.toUInt(),
    val containedBy: Set<URI> = emptySet()
) {
    companion object {
        const val DEFAULT_JOIN_LEVEL = 1

        enum class JoinType(val type: String) {
            FLAT("flat"),
            INLINE("inline"),
            NONE("@none");

            companion object {
                fun forType(type: String): JoinType? =
                    entries.find { it.type == type }
            }
        }

        fun parseLinkedEntityQueryParameters(
            join: String?,
            joinLevel: String?,
            containedBy: String?
        ): Either<APIException, LinkedEntityQuery?> = either {
            val containedBy = containedBy?.split(",").orEmpty().toListOfUri().toSet()
            val join = join?.let {
                JoinType.forType(it)?.right() ?: BadRequestDataException(badJoinParameterMessage(it)).left()
            }?.bind()
            val joinLevel = joinLevel?.let { param ->
                runCatching {
                    param.toUInt()
                }.fold(
                    { it.right() },
                    {
                        BadRequestDataException(badJoinLevelParameterMessage(param)).left()
                    }
                )
            }?.bind()

            if ((joinLevel != null || containedBy.isNotEmpty()) && join == null)
                raise(BadRequestDataException("'join' must be specified if 'joinLevel' or 'containedBy' are specified"))
            else join?.let { LinkedEntityQuery(it, joinLevel ?: DEFAULT_JOIN_LEVEL.toUInt(), containedBy) }
        }

        private fun badJoinParameterMessage(param: String) =
            "'$param' is not a recognized value for 'join' parameter (only 'flat', 'inline' and '@none' are allowed)"

        private fun badJoinLevelParameterMessage(param: String) =
            "'$param' is not a recognized value for 'joinLevel' parameter (only positive integers are allowed)"
    }
}
