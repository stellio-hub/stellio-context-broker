package com.egm.stellio.search.temporal.util

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.util.composeEntitiesQuery
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromPostRequest
import com.egm.stellio.search.entity.util.validateMinimalQueryEntitiesParameters
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.OptionsParamValue
import com.egm.stellio.shared.util.QUERY_PARAM_OPTIONS
import com.egm.stellio.shared.util.hasValueInOptionsParam
import com.egm.stellio.shared.util.parseTimeParameter
import org.springframework.util.MultiValueMap
import org.springframework.util.MultiValueMapAdapter
import java.time.ZonedDateTime
import java.util.*

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
        TIMEREL_PARAM to listOf(query.temporalQ?.timerel),
        TIMEAT_PARAM to listOf(query.temporalQ?.timeAt),
        ENDTIMEAT_PARAM to listOf(query.temporalQ?.endTimeAt),
        AGGRPERIODDURATION_PARAM to listOf(query.temporalQ?.aggrPeriodDuration),
        AGGRMETHODS_PARAM to query.temporalQ?.aggrMethods,
        LASTN_PARAM to listOf(query.temporalQ?.lastN.toString()),
        TIMEPROPERTY_PARAM to listOf(query.temporalQ?.timeproperty)
    )
    val temporalQuery = buildTemporalQuery(
        MultiValueMapAdapter(temporalParams),
        defaultPagination,
        true,
        withAggregatedValues
    ).bind()

    TemporalEntitiesQuery(
        entitiesQuery = entitiesQuery,
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        withAggregatedValues = withAggregatedValues
    )
}

@SuppressWarnings("ReturnCount")
fun buildTemporalQuery(
    params: MultiValueMap<String, String>,
    pagination: ApplicationProperties.Pagination,
    inQueryEntities: Boolean = false,
    withAggregatedValues: Boolean = false,
): Either<APIException, TemporalQuery> {
    val timerelParam = params.getFirst(TIMEREL_PARAM)
    val timeAtParam = params.getFirst(TIMEAT_PARAM)
    val endTimeAtParam = params.getFirst(ENDTIMEAT_PARAM)
    val aggrPeriodDurationParam =
        if (withAggregatedValues)
            params.getFirst(AGGRPERIODDURATION_PARAM) ?: WHOLE_TIME_RANGE_DURATION
        else null
    val aggrMethodsParam = params.getFirst(AGGRMETHODS_PARAM)
    val lastNParam = params.getFirst(LASTN_PARAM)
    val timeproperty = params.getFirst(TIMEPROPERTY_PARAM)?.let {
        AttributeInstance.TemporalProperty.forPropertyName(it)
    } ?: AttributeInstance.TemporalProperty.OBSERVED_AT

    val endTimeAt = endTimeAtParam?.parseTimeParameter("'endTimeAt' parameter is not a valid date")
        ?.getOrElse {
            return BadRequestDataException(it).left()
        }

    val (timerel, timeAt) = buildTimerelAndTime(timerelParam, timeAtParam, inQueryEntities).getOrElse {
        return BadRequestDataException(it).left()
    }

    if (timerel == TemporalQuery.Timerel.BETWEEN && endTimeAtParam == null)
        return BadRequestDataException("'endTimeAt' request parameter is mandatory if 'timerel' is 'between'").left()

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
    val limit = if (lastN != null && lastN < pagination.temporalLimit) lastN else pagination.temporalLimit

    return TemporalQuery(
        timerel = timerel,
        timeAt = timeAt,
        endTimeAt = endTimeAt,
        aggrPeriodDuration = aggrPeriodDurationParam,
        aggrMethods = aggregate,
        instanceLimit = limit,
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
