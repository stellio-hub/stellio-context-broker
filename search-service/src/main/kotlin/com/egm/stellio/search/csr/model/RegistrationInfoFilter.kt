package com.egm.stellio.search.csr.model

import java.net.URI

class RegistrationInfoFilter(
    ids: Set<URI> = emptySet(),
    val types: Set<String>? = null, // todo become TypeSelection
    idPattern: String? = null,
    operations: List<Operation>? = null
) : CSRFilters(
    ids = ids,
    typeSelection = types?.joinToString("|"),
    idPattern = idPattern,
    operations = operations
)
