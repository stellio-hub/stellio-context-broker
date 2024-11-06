package com.egm.stellio.search.csr.model

import java.net.URI

data class CSRFilters( // we should use a combination of EntitiesQuery TemporalQuery (when we implement all operations)
    val ids: Set<URI> = emptySet(),
    val operations: List<Operation>? = null,
    val csf: String? = null,
)
