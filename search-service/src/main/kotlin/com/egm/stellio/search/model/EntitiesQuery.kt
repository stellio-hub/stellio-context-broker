package com.egm.stellio.search.model

import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.util.ExpandedTerm
import java.net.URI

data class EntitiesQuery(
    val ids: Set<URI> = emptySet(),
    val type: String? = null,
    val idPattern: String? = null,
    val q: String? = null,
    val scopeQ: String? = null,
    val paginationQuery: PaginationQuery,
    val attrs: Set<ExpandedTerm> = emptySet(),
    val includeSysAttrs: Boolean = false,
    val useSimplifiedRepresentation: Boolean = false,
    val geoQuery: GeoQuery? = null,
    val context: String
)
