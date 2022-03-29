package com.egm.stellio.shared.model

import java.net.URI

data class QueryParams(
    val id: List<URI>? = null,
    val expandedType: String? = null,
    val idPattern: String? = null,
    val q: String? = null,
    val expandedAttrs: Set<String> = emptySet()
)
