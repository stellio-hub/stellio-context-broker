package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.model.PaginationQuery
import java.net.URI

sealed class EntitiesQuery(
    open val q: String? = null,
    open val scopeQ: String? = null,
    open val paginationQuery: PaginationQuery,
    open val attrs: Set<ExpandedTerm> = emptySet(),
    open val datasetId: Set<String> = emptySet(),
    open val geoQuery: GeoQuery? = null,
    open val contexts: List<String>
)

data class EntitiesQueryFromGet(
    val ids: Set<URI> = emptySet(),
    val typeSelection: EntityTypeSelection? = null,
    val idPattern: String? = null,
    override val q: String? = null,
    override val scopeQ: String? = null,
    override val paginationQuery: PaginationQuery,
    override val attrs: Set<ExpandedTerm> = emptySet(),
    override val datasetId: Set<String> = emptySet(),
    override val geoQuery: GeoQuery? = null,
    override val contexts: List<String>
) : EntitiesQuery(q, scopeQ, paginationQuery, attrs, datasetId, geoQuery, contexts)

data class EntitiesQueryFromPost(
    val entitySelectors: List<EntitySelector>?,
    override val q: String? = null,
    override val scopeQ: String? = null,
    override val paginationQuery: PaginationQuery,
    override val attrs: Set<ExpandedTerm> = emptySet(),
    override val datasetId: Set<String> = emptySet(),
    override val geoQuery: GeoQuery? = null,
    override val contexts: List<String>
) : EntitiesQuery(q, scopeQ, paginationQuery, attrs, datasetId, geoQuery, contexts)
