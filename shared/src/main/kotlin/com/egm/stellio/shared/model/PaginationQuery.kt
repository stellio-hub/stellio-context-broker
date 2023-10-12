package com.egm.stellio.shared.model

data class PaginationQuery(
    val offset: Int,
    val limit: Int,
    val count: Boolean = false
)
