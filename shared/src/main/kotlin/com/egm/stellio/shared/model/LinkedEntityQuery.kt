package com.egm.stellio.shared.model

import java.net.URI
import kotlin.UInt

data class LinkedEntityQuery(
    val join: JoinType = JoinType.NONE,
    val joinLevel: UInt = DEFAULT_JOIN_LEVEL.toUInt(),
    val containedBy: Set<URI> = emptySet()
) {
    companion object {
        const val DEFAULT_JOIN_LEVEL = 1
    }

    enum class JoinType(val join: String) {
        FLAT("flat"),
        INLINE("inline"),
        NONE("@none");

        companion object {
            fun isSupportedType(join: String): Boolean =
                entries.any { it.join == join }

            fun forType(join: String): JoinType? =
                entries.find { it.join == join }
        }
    }
}
