package com.egm.stellio.search.csr.model

import java.net.URI

data class CSRFilters( // we should use a combination of EntitiesQuery TemporalQuery (when we implement all operations)
    val ids: Set<URI> = emptySet(),
    val operations: List<Operation>? = null,
    val csf: String? = null,
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
        val operationRegex = "operation==([a-zA-Z])+\$".toRegex()
        val operations = operations ?: csf?.let {
            operationRegex.find(it)?.value?.let { op -> listOf(Operation.fromString(op)) }
        }

        val csfFilter = operations?.let {
            "operations && ARRAY[${it.joinToString(",") { op -> "'${op?.key}'" }}]"
        } ?: "true"

        return """
            $idFilter 
            AND
            $csfFilter
        """.trimMargin()
    }
}
