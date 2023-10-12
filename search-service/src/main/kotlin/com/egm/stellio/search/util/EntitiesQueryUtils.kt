package com.egm.stellio.search.util

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.model.*
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import org.springframework.util.MultiValueMap
import java.time.ZonedDateTime
import java.util.Optional
import java.util.regex.Pattern

fun parseQueryParams(
    pagination: Pair<Int, Int>,
    requestParams: MultiValueMap<String, String>,
    contextLink: String
): Either<APIException, EntitiesQuery> = either {
    val ids = requestParams.getFirst(QUERY_PARAM_ID)?.split(",").orEmpty().toListOfUri().toSet()
    val type = parseAndExpandTypeSelection(requestParams.getFirst(QUERY_PARAM_TYPE), contextLink)
    val idPattern = requestParams.getFirst(QUERY_PARAM_ID_PATTERN)?.also { idPattern ->
        runCatching {
            Pattern.compile(idPattern)
        }.onFailure {
            BadRequestDataException("Invalid value for idPattern: $idPattern ($it)").left().bind()
        }
    }

    /**
     * Decoding query parameters is not supported by default so a call to a decode function was added query
     * with the right parameters values
     */
    val q = requestParams.getFirst(QUERY_PARAM_Q)?.decode()
    val scopeQ = requestParams.getFirst(QUERY_PARAM_SCOPEQ)
    val attrs = parseAndExpandRequestParameter(requestParams.getFirst(QUERY_PARAM_ATTRS), contextLink)
    val includeSysAttrs = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
    val useSimplifiedRepresentation = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
    val paginationQuery = parsePaginationParameters(
        requestParams,
        pagination.first,
        pagination.second
    ).bind()

    val geoQuery = parseGeoQueryParameters(requestParams.toSingleValueMap(), contextLink).bind()

    EntitiesQuery(
        ids = ids,
        type = type,
        idPattern = idPattern,
        q = q,
        scopeQ = scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        includeSysAttrs = includeSysAttrs,
        useSimplifiedRepresentation = useSimplifiedRepresentation,
        geoQuery = geoQuery,
        context = contextLink
    )
}

fun EntitiesQuery.validateMinimalQueryEntitiesParameters(): Either<APIException, EntitiesQuery> = either {
    if (
        ids.isEmpty() &&
        q.isNullOrEmpty() &&
        type.isNullOrEmpty() &&
        attrs.isEmpty()
    )
        return@either BadRequestDataException(
            "one of 'ids', 'q', 'type' and 'attrs' request parameters have to be specified"
        ).left().bind<EntitiesQuery>()

    this@validateMinimalQueryEntitiesParameters
}

fun parseQueryParamsForPost(
    pagination: Pair<Int, Int>,
    requestBody: String,
    requestParams: MultiValueMap<String, String>,
    contextLink: String
): Either<APIException, EntitiesQuery> = either {
    val query = Query(requestBody).bind()
    val entitySelector = query.entities?.get(0)
    val id = entitySelector?.id?.toUri()
    val type = parseAndExpandTypeSelection(entitySelector?.type, contextLink)
    val idPattern = entitySelector?.idPattern?.also { idPattern ->
        runCatching {
            Pattern.compile(idPattern)
        }.onFailure {
            BadRequestDataException("Invalid value for idPattern: $idPattern ($it)").left().bind()
        }
    }
    val attrs = parseAndExpandRequestParameter(query.attrs?.joinToString(","), contextLink)
    val geoQuery = if (query.geoQ != null) {
        val geoQueryElements = mapOf(
            "geometry" to query.geoQ.geometry,
            "coordinates" to query.geoQ.coordinates.toString(),
            "georel" to query.geoQ.georel,
            "geoproperty" to query.geoQ.geoproperty
        )
        parseGeoQueryParameters(geoQueryElements, contextLink).bind()
    } else null

    val includeSysAttrs = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
    val useSimplifiedRepresentation = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
    val paginationQuery = parsePaginationParameters(
        requestParams,
        pagination.first,
        pagination.second
    ).bind()

    EntitiesQuery(
        ids = setOfNotNull(id),
        type = type,
        idPattern = idPattern,
        q = query.q?.decode(),
        scopeQ = query.scopeQ,
        paginationQuery = paginationQuery,
        attrs = attrs,
        includeSysAttrs = includeSysAttrs,
        useSimplifiedRepresentation = useSimplifiedRepresentation,
        geoQuery = geoQuery,
        context = contextLink
    )
}

fun parseQueryAndTemporalParams(
    pagination: ApplicationProperties.Pagination,
    requestParams: MultiValueMap<String, String>,
    contextLink: String,
    inQueryEntities: Boolean = false
): Either<APIException, TemporalEntitiesQuery> = either {
    val queryParams = parseQueryParams(
        Pair(pagination.limitDefault, pagination.limitMax),
        requestParams,
        contextLink
    ).bind()

    if (queryParams.type.isNullOrEmpty() && queryParams.attrs.isEmpty() && inQueryEntities)
        BadRequestDataException("Either type or attrs need to be present in request parameters")
            .left().bind<TemporalEntitiesQuery>()

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
    val temporalQuery = buildTemporalQuery(requestParams, inQueryEntities, withAggregatedValues).bind()

    TemporalEntitiesQuery(
        entitiesQuery = queryParams,
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        withAggregatedValues = withAggregatedValues
    )
}

fun buildTemporalQuery(
    params: MultiValueMap<String, String>,
    inQueryEntities: Boolean = false,
    withAggregatedValues: Boolean = false
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

    val lastN = lastNParam?.toIntOrNull()?.let {
        if (it >= 1) it else null
    }

    return TemporalQuery(
        timerel = timerel,
        timeAt = timeAt,
        endTimeAt = endTimeAt,
        aggrPeriodDuration = aggrPeriodDurationParam,
        aggrMethods = aggregate,
        lastN = lastN,
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
