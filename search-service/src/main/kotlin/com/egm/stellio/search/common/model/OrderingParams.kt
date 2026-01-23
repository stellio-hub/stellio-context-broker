package com.egm.stellio.search.common.model

import arrow.core.raise.either

/**
 * An OrderingParams data type as defined in 5.2.43.
 *
 */
data class OrderingParams(
    val orderBy: List<OrderBy> = emptyList(),
    val collation: String? = null,
    val geometry: String? = null,
    val coordinates: List<Any>? = null,
) {

    companion object {
        fun fromUnparsedOrderBy(unparsedOrderBy: List<String>?, contexts: List<String>) = either {
            OrderingParams(orderBy = unparsedOrderBy?.map { OrderBy.fromParam(it, contexts).bind() } ?: emptyList())
        }
    }

    fun toSQL() = if (orderBy.isNotEmpty())
        orderBy.joinToString(", ") { it.buildSql() }
    else "entity_payload.created_at"
}
