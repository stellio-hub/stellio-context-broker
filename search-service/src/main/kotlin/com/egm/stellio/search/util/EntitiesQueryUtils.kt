package com.egm.stellio.search.util

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.model.*
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.TooManyResultsException
import com.egm.stellio.shared.util.*
import org.springframework.util.MultiValueMap
import org.springframework.util.MultiValueMapAdapter
import java.time.ZonedDateTime
import java.util.*

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
    val attrs = parseAndExpandRequestParameter(query.attrs?.joinToString(","), contexts)
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
        geoQuery = geoQuery,
        contexts = contexts
    )
}

fun composeTemporalEntitiesQuery(
    defaultPagination: ApplicationProperties.Pagination,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>,
    inQueryEntities: Boolean = false
): Either<APIException, TemporalEntitiesQuery> = either {
    val entitiesQuery = composeEntitiesQuery(
        defaultPagination,
        requestParams,
        contexts
    ).bind()

    if (inQueryEntities)
        entitiesQuery.validateMinimalQueryEntitiesParameters().bind()

    val withTemporalValues = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.TEMPORAL_VALUES
    )
    val withAudit = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.AUDIT
    )
    val withAggregatedValues = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.AGGREGATED_VALUES
    )
    val temporalQuery =
        buildTemporalQuery(requestParams, defaultPagination, inQueryEntities, withAggregatedValues).bind()

    TemporalEntitiesQuery(
        entitiesQuery = entitiesQuery,
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        withAggregatedValues = withAggregatedValues
    )
}

fun composeTemporalEntitiesQueryFromPostRequest(
    defaultPagination: ApplicationProperties.Pagination,
    query: Query,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, TemporalEntitiesQuery> = either {
    val entitiesQuery = composeEntitiesQueryFromPostRequest(
        defaultPagination,
        query,
        requestParams,
        contexts
    ).bind()
        .validateMinimalQueryEntitiesParameters().bind()

    val withTemporalValues = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.TEMPORAL_VALUES
    )
    val withAudit = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.AUDIT
    )
    val withAggregatedValues = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.AGGREGATED_VALUES
    )

    val temporalParams = mapOf(
        "timerel" to listOf(query.temporalQ?.timerel),
        "timeAt" to listOf(query.temporalQ?.timeAt),
        "endTimeAt" to listOf(query.temporalQ?.endTimeAt),
        "aggrPeriodDuration" to listOf(query.temporalQ?.aggrPeriodDuration),
        "aggrMethods" to query.temporalQ?.aggrMethods,
        "lastN" to listOf(query.temporalQ?.lastN.toString()),
        "timeproperty" to listOf(query.temporalQ?.timeproperty)
    )
    val temporalQuery = buildTemporalQuery(
        MultiValueMapAdapter(temporalParams),
        defaultPagination,
        true,
        withAggregatedValues,
    ).bind()

    TemporalEntitiesQuery(
        entitiesQuery = entitiesQuery,
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        withAggregatedValues = withAggregatedValues
    )
}

fun buildTemporalQuery(
    params: MultiValueMap<String, String>,
    pagination: ApplicationProperties.Pagination,
    inQueryEntities: Boolean = false,
    withAggregatedValues: Boolean = false,
): Either<APIException, TemporalQuery> {
    val timerelParam = params.getFirst("timerel")
    val timeAtParam = params.getFirst("timeAt")
    val endTimeAtParam = params.getFirst("endTimeAt")
    val aggrPeriodDurationParam =
        if (withAggregatedValues)
            params.getFirst("aggrPeriodDuration") ?: "PT0S"
        else null
    val aggrMethodsParam = params.getFirst("aggrMethods")
    val lastNParam = params.getFirst("lastN")
    val timeproperty = params.getFirst("timeproperty")?.let {
        AttributeInstance.TemporalProperty.forPropertyName(it)
    } ?: AttributeInstance.TemporalProperty.OBSERVED_AT

    if (timerelParam == "between" && endTimeAtParam == null)
        return BadRequestDataException("'endTimeAt' request parameter is mandatory if 'timerel' is 'between'").left()

    val endTimeAt = endTimeAtParam?.parseTimeParameter("'endTimeAt' parameter is not a valid date")
        ?.getOrElse {
            return BadRequestDataException(it).left()
        }

    val (timerel, timeAt) = buildTimerelAndTime(timerelParam, timeAtParam, inQueryEntities).getOrElse {
        return BadRequestDataException(it).left()
    }
    if (withAggregatedValues && aggrMethodsParam == null)
        return BadRequestDataException("'aggrMethods' is mandatory if 'aggregatedValues' option is specified").left()

    val aggregate = aggrMethodsParam?.split(",")?.map {
        if (TemporalQuery.Aggregate.isSupportedAggregate(it))
            TemporalQuery.Aggregate.forMethod(it)!!
        else
            return BadRequestDataException(
                "'$it' is not a recognized aggregation method for 'aggrMethods' parameter"
            ).left()
    }
    val isChronological = lastNParam == null
    val limit = lastNParam?.toIntOrNull()?.let {
        if (it > pagination.temporalLimitMax) return TooManyResultsException(
            "You asked for the $it last temporal entities, but the supported maximum limit is ${
                pagination.temporalLimitMax
            }"
        ).left()
        else if (it >= 1) it
        else pagination.temporalLimitDefault
    } ?: pagination.temporalLimitDefault

    return TemporalQuery(
        timerel = timerel,
        timeAt = timeAt,
        endTimeAt = endTimeAt,
        aggrPeriodDuration = aggrPeriodDurationParam,
        aggrMethods = aggregate,
        limit = limit,
        isChronological = isChronological,
        timeproperty = timeproperty
    ).right()
}

fun buildTimerelAndTime(
    timerelParam: String?,
    timeAtParam: String?,
    inQueryEntities: Boolean
): Either<String, Pair<TemporalQuery.Timerel?, ZonedDateTime?>> =
    // when querying a specific temporal entity, timeAt and timerel are optional
    if (timerelParam == null && timeAtParam == null && !inQueryEntities) {
        Pair(null, null).right()
    } else if (timerelParam != null && timeAtParam != null) {
        val timeRelResult = try {
            TemporalQuery.Timerel.valueOf(timerelParam.uppercase()).right()
        } catch (e: IllegalArgumentException) {
            "'timerel' is not valid, it should be one of 'before', 'between', or 'after'".left()
        }

        timeRelResult.flatMap { timerel ->
            timeAtParam.parseTimeParameter("'timeAt' parameter is not a valid date")
                .map {
                    Pair(timerel, it)
                }
        }
    } else {
        "'timerel' and 'time' must be used in conjunction".left()
    }
