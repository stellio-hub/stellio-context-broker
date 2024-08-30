package com.egm.stellio.search.entity.util

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import org.springframework.util.MultiValueMap

fun composeEntitiesQuery(
    defaultPagination: ApplicationProperties.Pagination,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, EntitiesQuery> = either {
    val ids = requestParams.getFirst(QUERY_PARAM_ID)?.split(",").orEmpty().toListOfUri().toSet()
    val typeSelection = expandTypeSelection(requestParams.getFirst(QUERY_PARAM_TYPE), contexts)
    val idPattern = validateIdPattern(requestParams.getFirst(QUERY_PARAM_ID_PATTERN)).bind()

    /**
     * Decoding query parameters is not supported by default so a call to a decode function was added query
     * with the right parameters values
     */
    val q = requestParams.getFirst(QUERY_PARAM_Q)?.decode()
    val scopeQ = requestParams.getFirst(QUERY_PARAM_SCOPEQ)
    val attrs = parseAndExpandRequestParameter(requestParams.getFirst(QUERY_PARAM_ATTRS), contexts)
    val datasetId = parseRequestParameter(requestParams.getFirst(QUERY_PARAM_DATASET_ID))
    val paginationQuery = parsePaginationParameters(
        requestParams,
        defaultPagination.limitDefault,
        defaultPagination.limitMax
    ).bind()

    val geoQuery = parseGeoQueryParameters(requestParams.toSingleValueMap(), contexts).bind()

    EntitiesQuery(
        ids = ids,
        typeSelection = typeSelection,
        idPattern = idPattern,
        q = q,
        scopeQ = scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        datasetId = datasetId,
        geoQuery = geoQuery,
        contexts = contexts
    )
}

fun EntitiesQuery.validateMinimalQueryEntitiesParameters(): Either<APIException, EntitiesQuery> = either {
    if (
        geoQuery == null &&
        q.isNullOrEmpty() &&
        typeSelection.isNullOrEmpty() &&
        attrs.isEmpty()
    )
        return@either BadRequestDataException(
            "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query"
        ).left().bind<EntitiesQuery>()

    this@validateMinimalQueryEntitiesParameters
}

fun composeEntitiesQueryFromPostRequest(
    defaultPagination: ApplicationProperties.Pagination,
    query: Query,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, EntitiesQuery> = either {
    val entitySelector = query.entities?.get(0)
    val typeSelection = expandTypeSelection(entitySelector?.typeSelection, contexts)
    val idPattern = validateIdPattern(entitySelector?.idPattern).bind()
    val attrs = query.attrs.orEmpty().map { JsonLdUtils.expandJsonLdTerm(it.trim(), contexts) }.toSet()
    val datasetId = query.datasetId.orEmpty().toSet()
    val geoQuery = if (query.geoQ != null) {
        val geoQueryElements = mapOf(
            "geometry" to query.geoQ.geometry,
            "coordinates" to query.geoQ.coordinates.toString(),
            "georel" to query.geoQ.georel,
            "geoproperty" to query.geoQ.geoproperty
        )
        parseGeoQueryParameters(geoQueryElements, contexts).bind()
    } else null

    val paginationQuery = parsePaginationParameters(
        requestParams,
        defaultPagination.limitDefault,
        defaultPagination.limitMax
    ).bind()

    EntitiesQuery(
        ids = setOfNotNull(entitySelector?.id),
        typeSelection = typeSelection,
        idPattern = idPattern,
        q = query.q?.decode(),
        scopeQ = query.scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        datasetId = datasetId,
        geoQuery = geoQuery,
        contexts = contexts
    )
}
