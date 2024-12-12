package com.egm.stellio.search.csr.model

import java.net.URI

data class CSRFilters( // we should use a combination of EntitiesQuery TemporalQuery (when we implement all operations)
    val ids: Set<URI> = emptySet(),
    val type: String? = null,
    val idPattern: String? = null,
    val csf: String? = null,
) {
    constructor(
        ids: Set<URI> = emptySet(),
        type: String? = null,
        idPattern: String? = null,
        operations: List<Operation>
    ) :
        this(
            ids = ids,
            type = type,
            idPattern = idPattern,
            csf = operations.joinToString("|") { "${ContextSourceRegistration::operations.name}==${it.key}" }
        )
}
