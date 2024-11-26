package com.egm.stellio.shared.model.parameter

enum class PaginationParameter(
    override val key: String,
    override val implemented: Boolean = true,
) : QueryParameter {
    COUNT("count",),
    OFFSET("offset"),
    LIMIT("limit")
}
