package com.egm.stellio.search.entity.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.QUERY_PARAM_ATTRS
import com.egm.stellio.shared.util.QUERY_PARAM_CONTAINED_BY
import com.egm.stellio.shared.util.QUERY_PARAM_DATASET_ID
import com.egm.stellio.shared.util.QUERY_PARAM_ID
import com.egm.stellio.shared.util.QUERY_PARAM_ID_PATTERN
import com.egm.stellio.shared.util.QUERY_PARAM_JOIN
import com.egm.stellio.shared.util.QUERY_PARAM_JOIN_LEVEL
import com.egm.stellio.shared.util.QUERY_PARAM_Q
import com.egm.stellio.shared.util.QUERY_PARAM_SCOPEQ
import com.egm.stellio.shared.util.QUERY_PARAM_TYPE
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.shared.util.parseAndExpandRequestParameter
import com.egm.stellio.shared.model.param.parseGeoQueryParameters
import com.egm.stellio.shared.util.parseLinkedEntityQueryParameters
import com.egm.stellio.shared.util.parsePaginationParameters
import com.egm.stellio.shared.util.parseRequestParameter
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.validateIdPattern
import org.springframework.util.MultiValueMap

fun composeEntitiesQueryFromGet(
    defaultPagination: ApplicationProperties.Pagination,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, EntitiesQueryFromGet> = either {
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
    val linkedEntityQuery = parseLinkedEntityQueryParameters(
        requestParams.getFirst(QUERY_PARAM_JOIN),
        requestParams.getFirst(QUERY_PARAM_JOIN_LEVEL),
        requestParams.getFirst(QUERY_PARAM_CONTAINED_BY)
    ).bind()

    EntitiesQueryFromGet(
        ids = ids,
        typeSelection = typeSelection,
        idPattern = idPattern,
        q = q,
        scopeQ = scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        datasetId = datasetId,
        geoQuery = geoQuery,
        linkedEntityQuery = linkedEntityQuery,
        contexts = contexts
    )
}

fun EntitiesQueryFromGet.validateMinimalQueryEntitiesParameters(): Either<APIException, EntitiesQueryFromGet> = either {
    if (
        geoQuery == null &&
        q.isNullOrEmpty() &&
        typeSelection.isNullOrEmpty() &&
        attrs.isEmpty()
    )
        return@either BadRequestDataException(
            "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query"
        ).left().bind<EntitiesQueryFromGet>()

    this@validateMinimalQueryEntitiesParameters
}

fun composeEntitiesQueryFromPost(
    defaultPagination: ApplicationProperties.Pagination,
    query: Query,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, EntitiesQueryFromPost> = either {
    val entitySelectors = query.entities?.map { entitySelector ->
        validateIdPattern(entitySelector.idPattern).bind()
        EntitySelector(
            entitySelector.id,
            entitySelector.idPattern,
            expandTypeSelection(entitySelector.typeSelection, contexts)!!
        )
    }
    val attrs = query.attrs.orEmpty().map { JsonLdUtils.expandJsonLdTerm(it.trim(), contexts) }.toSet()
    val datasetId = query.datasetId.orEmpty().toSet()
    val geoQuery = query.geoQ?.let {
        val geoQueryElements = mapOf(
            "geometry" to query.geoQ.geometry,
            "coordinates" to query.geoQ.coordinates.toString(),
            "georel" to query.geoQ.georel,
            "geoproperty" to query.geoQ.geoproperty
        )
        parseGeoQueryParameters(geoQueryElements, contexts).bind()
    }
    val linkedEntityQuery = parseLinkedEntityQueryParameters(
        query.join,
        query.joinLevel?.toString(),
        query.containedBy?.joinToString(",")
    ).bind()

    val paginationQuery = parsePaginationParameters(
        requestParams,
        defaultPagination.limitDefault,
        defaultPagination.limitMax
    ).bind()

    EntitiesQueryFromPost(
        entitySelectors = entitySelectors,
        q = query.q?.decode(),
        scopeQ = query.scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        datasetId = datasetId,
        geoQuery = geoQuery,
        linkedEntityQuery = linkedEntityQuery,
        contexts = contexts
    )
}
