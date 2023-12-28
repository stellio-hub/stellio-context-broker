package com.egm.stellio.search.model

import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.model.PaginationQuery
import java.net.URI

data class EntitiesQuery(
    val ids: Set<URI> = emptySet(),
    val typeSelection: EntityTypeSelection? = null,
    val idPattern: String? = null,
    val q: String? = null,
    val scopeQ: String? = null,
    val paginationQuery: PaginationQuery,
    val attrs: Set<ExpandedTerm> = emptySet(),
    val geoQuery: GeoQuery? = null,
    val context: String
)
