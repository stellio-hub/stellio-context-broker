package com.egm.stellio.search.csr.model

import java.net.URI

data class CSRFilters( // we should use a combination of EntitiesQuery TemporalQuery (when we implement all operations)
    val ids: Set<URI> = emptySet(),
    val csf: String? = null,
) {
    constructor(ids: Set<URI> = emptySet(), operations: List<Operation>) :
        this(
            ids = ids,
            csf = operations.joinToString("|") { "${ContextSourceRegistration::operations.name}==${it.key}" }
        )
}
