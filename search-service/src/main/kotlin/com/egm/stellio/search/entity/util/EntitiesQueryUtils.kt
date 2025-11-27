package com.egm.stellio.search.entity.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.queryparameter.GeoQuery.Companion.parseGeoQueryParameters
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.parseLinkedEntityQueryParameters
import com.egm.stellio.shared.queryparameter.PaginationQuery.Companion.parsePaginationParameters
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.ApiUtils.expandTypeSelection
import com.egm.stellio.shared.util.ApiUtils.parseAttrsParameter
import com.egm.stellio.shared.util.ApiUtils.parsePickOmitParameters
import com.egm.stellio.shared.util.ApiUtils.parseQueryParameter
import com.egm.stellio.shared.util.ApiUtils.validateIdPattern
import com.egm.stellio.shared.util.HttpUtils.decode
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.UriUtils.toListOfUri
import org.springframework.util.MultiValueMap

fun composeEntitiesQueryFromGet(
    defaultPagination: ApplicationProperties.Pagination,
    queryParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, EntitiesQueryFromGet> = either {
    val ids = queryParams.getFirst(QueryParameter.ID.key)?.split(",").orEmpty().toListOfUri().toSet()
    val typeSelection = expandTypeSelection(queryParams.getFirst(QueryParameter.TYPE.key), contexts)
    val idPattern = validateIdPattern(queryParams.getFirst(QueryParameter.ID_PATTERN.key)).bind()

    /**
     * Decoding query parameters is not supported by default so a call to a decode function was added query
     * with the right parameters values
     */
    val q = queryParams.getFirst(QueryParameter.Q.key)?.decode()
    val scopeQ = queryParams.getFirst(QueryParameter.SCOPEQ.key)
    val attrs = parseAttrsParameter(queryParams.getFirst(QueryParameter.ATTRS.key), contexts).bind()
    val (pick, omit) = parsePickOmitParameters(
        queryParams.getFirst(QueryParameter.PICK.key),
        queryParams.getFirst(QueryParameter.OMIT.key)
    ).bind()
    validateMutualAttrsProjectionAttributesExclusion(attrs, pick, omit).bind()
    val datasetId = parseQueryParameter(queryParams.getFirst(QueryParameter.DATASET_ID.key))
    val paginationQuery = parsePaginationParameters(
        queryParams,
        defaultPagination.limitDefault,
        defaultPagination.limitMax
    ).bind()

    val geoQuery = parseGeoQueryParameters(queryParams.toSingleValueMap(), contexts).bind()
    val linkedEntityQuery = parseLinkedEntityQueryParameters(
        queryParams.getFirst(QueryParameter.JOIN.key),
        queryParams.getFirst(QueryParameter.JOIN_LEVEL.key),
        queryParams.getFirst(QueryParameter.CONTAINED_BY.key)
    ).bind()
    val local = queryParams.getFirst(QueryParameter.LOCAL.key)?.toBoolean() ?: false

    EntitiesQueryFromGet(
        ids = ids,
        typeSelection = typeSelection,
        idPattern = idPattern,
        q = q,
        scopeQ = scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        pick = pick,
        omit = omit,
        datasetId = datasetId,
        geoQuery = geoQuery,
        linkedEntityQuery = linkedEntityQuery,
        local = local,
        contexts = contexts
    )
}

fun EntitiesQueryFromGet.validateMinimalQueryEntitiesParameters(): Either<APIException, EntitiesQueryFromGet> = either {
    if (
        geoQuery == null &&
        q.isNullOrEmpty() &&
        typeSelection.isNullOrEmpty() &&
        attrs.isEmpty() &&
        !local
    )
        return@either BadRequestDataException(
            "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query unless local is true"
        ).left().bind<EntitiesQueryFromGet>()

    this@validateMinimalQueryEntitiesParameters
}

fun composeEntitiesQueryFromPost(
    defaultPagination: ApplicationProperties.Pagination,
    query: Query,
    queryParams: MultiValueMap<String, String>,
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
    val (pick, omit) = parsePickOmitParameters(
        query.pick?.joinToString(","),
        query.omit?.joinToString(",")
    ).bind()
    validateMutualAttrsProjectionAttributesExclusion(attrs, pick, omit).bind()
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
        queryParams,
        defaultPagination.limitDefault,
        defaultPagination.limitMax
    ).bind()

    EntitiesQueryFromPost(
        entitySelectors = entitySelectors,
        q = query.q?.decode(),
        scopeQ = query.scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        pick = pick,
        omit = omit,
        datasetId = datasetId,
        geoQuery = geoQuery,
        linkedEntityQuery = linkedEntityQuery,
        contexts = contexts
    )
}

fun validateMutualAttrsProjectionAttributesExclusion(
    attrs: Set<String>,
    pick: Set<String>,
    omit: Set<String>
): Either<APIException, Unit> =
    if (attrs.isNotEmpty() && (pick.isNotEmpty() || omit.isNotEmpty()))
        BadRequestDataException(
            "The 'attrs' parameter cannot be used together with 'pick' or 'omit' parameters"
        ).left()
    else if (pick.intersect(omit).isNotEmpty())
        BadRequestDataException(
            "An entity member cannot be present in both 'pick' and 'omit' parameters"
        ).left()
    else Unit.right()
