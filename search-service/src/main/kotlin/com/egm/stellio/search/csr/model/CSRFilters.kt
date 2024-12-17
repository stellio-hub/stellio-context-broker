package com.egm.stellio.search.csr.model

import com.egm.stellio.shared.model.EntityTypeSelection
import java.net.URI

data class CSRFilters( // we should use a combination of EntitiesQuery TemporalQuery (when we implement all operations)
    val ids: Set<URI> = emptySet(),
    val typeSelection: EntityTypeSelection? = null,
    val idPattern: String? = null,
    val csf: String? = null,
) {
    constructor(
        ids: Set<URI> = emptySet(),
        typeSelection: EntityTypeSelection? = null,
        idPattern: String? = null,
        operations: List<Operation>
    ) :
        this(
            ids = ids,
            typeSelection = typeSelection,
            idPattern = idPattern,
            csf = operations.joinToString("|") { "${ContextSourceRegistration::operations.name}==${it.key}" }
        )
}
