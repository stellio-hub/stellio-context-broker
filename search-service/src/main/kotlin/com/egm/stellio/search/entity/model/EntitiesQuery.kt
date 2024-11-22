package com.egm.stellio.search.entity.model

import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.param.GeoQuery
import com.egm.stellio.shared.model.LinkedEntityQuery
import com.egm.stellio.shared.model.PaginationQuery
import java.net.URI

sealed class EntitiesQuery(
    open val q: String?,
    open val scopeQ: String?,
    open val paginationQuery: PaginationQuery,
    open val attrs: Set<ExpandedTerm>,
    open val datasetId: Set<String>,
    open val geoQuery: GeoQuery?,
    open val linkedEntityQuery: LinkedEntityQuery?,
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
    override val linkedEntityQuery: LinkedEntityQuery? = null,
    override val contexts: List<String>
) : EntitiesQuery(q, scopeQ, paginationQuery, attrs, datasetId, geoQuery, linkedEntityQuery, contexts)

data class EntitiesQueryFromPost(
    val entitySelectors: List<EntitySelector>? = null,
    override val q: String? = null,
    override val scopeQ: String? = null,
    override val paginationQuery: PaginationQuery,
    override val attrs: Set<ExpandedTerm> = emptySet(),
    override val datasetId: Set<String> = emptySet(),
    override val geoQuery: GeoQuery? = null,
    override val linkedEntityQuery: LinkedEntityQuery? = null,
    override val contexts: List<String>
) : EntitiesQuery(q, scopeQ, paginationQuery, attrs, datasetId, geoQuery, linkedEntityQuery, contexts)
