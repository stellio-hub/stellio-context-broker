package com.egm.stellio.search.common.model

/**
 * A Query data type as defined in 5.2.23.
 *
 */
data class OrderingParams(
    val orderBy: List<OrderBy> = emptyList(),
    val collation: String? = null,
    val geometry: String? = null,
    val coordinate: List<Any>? = null,
) {

    constructor(unparsedOrderBy: List<String>?, contexts: List<String>) : this(
        unparsedOrderBy?.map { OrderBy(it, contexts) } ?: emptyList()
    )

    fun toSQL() = if (orderBy.isNotEmpty())
        orderBy.joinToString(", ") { it.buildSql() }
    else "entity_payload.entity_id"
}
