package com.egm.stellio.search.csr.model

import java.net.URI

data class CSRFilters( // we should use a combination of EntitiesQuery TemporalQuery (when we implement all operations)
    val ids: Set<URI> = emptySet(),
    val csf: String? = null
) {
    fun buildWhereStatement(): String {
        val idFilter = if (ids.isNotEmpty())
            """
            (
                entity_info.id is null OR
                entity_info.id in ('${ids.joinToString("', '")}')
            ) AND
            (
                entity_info.idPattern is null OR 
                ${ids.joinToString(" OR ") { "'$it' ~ entity_info.idPattern" }}
            )
            """.trimIndent()
        else "true"
        return idFilter
    }
}
