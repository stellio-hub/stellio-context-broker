package com.egm.stellio.shared.model

data class QueryParams(
    val id: List<String>? = null,
    val expandedType: String? = null,
    val idPattern: String? = null,
    val q: String? = null
)
