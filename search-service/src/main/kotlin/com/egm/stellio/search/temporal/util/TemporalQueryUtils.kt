package com.egm.stellio.search.temporal.util

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromGet
import com.egm.stellio.search.entity.util.composeEntitiesQueryFromPost
import com.egm.stellio.search.entity.util.validateMinimalQueryEntitiesParameters
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromPost
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.model.TemporalQuery.Aggregate
import com.egm.stellio.search.temporal.model.TemporalQuery.Timerel
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.queryparameter.FormatValue
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.QueryParamValue
import com.egm.stellio.shared.util.hasValueInQueryParam
import com.egm.stellio.shared.util.parseTimeParameter
import org.springframework.util.MultiValueMap
import org.springframework.util.MultiValueMapAdapter
import java.time.ZonedDateTime
import java.util.Optional

const val WHOLE_TIME_RANGE_DURATION = "PT0S"

fun composeTemporalEntitiesQueryFromGet(
    defaultPagination: ApplicationProperties.Pagination,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>,
    inQueryEntities: Boolean = false
): Either<APIException, TemporalEntitiesQueryFromGet> = either {
    val entitiesQueryFromGet = composeEntitiesQueryFromGet(
        defaultPagination,
        requestParams,
        contexts
    ).bind()
    if (inQueryEntities) {
        entitiesQueryFromGet.validateMinimalQueryEntitiesParameters().bind()
    }
    val temporalRepresentation = extractTemporalRepresentation(requestParams).bind()
    val withAudit = hasValueInQueryParam(
        Optional.ofNullable(requestParams.getFirst(QueryParameter.OPTIONS.key)),
        QueryParamValue.AUDIT
    )
    val temporalQuery =
        buildTemporalQuery(requestParams, defaultPagination, inQueryEntities, temporalRepresentation).bind()

    TemporalEntitiesQueryFromGet(
        entitiesQuery = entitiesQueryFromGet,
        temporalQuery = temporalQuery,
        temporalRepresentation = temporalRepresentation,
        withAudit = withAudit
    )
}

fun composeTemporalEntitiesQueryFromPost(
    defaultPagination: ApplicationProperties.Pagination,
    query: Query,
    requestParams: MultiValueMap<String, String>,
    contexts: List<String>
): Either<APIException, TemporalEntitiesQuery> = either {
    val entitiesQueryFromPost = composeEntitiesQueryFromPost(
        defaultPagination,
        query,
        requestParams,
        contexts
    ).bind()
    val temporalRepresentation = extractTemporalRepresentation(requestParams).bind()

    val withAudit = hasValueInQueryParam(
        Optional.ofNullable(requestParams.getFirst(QueryParameter.OPTIONS.key)),
        QueryParamValue.AUDIT
    )
    val temporalParams = mapOf(
        QueryParameter.TIMEREL.key to listOf(query.temporalQ?.timerel),
        QueryParameter.TIMEAT.key to listOf(query.temporalQ?.timeAt),
        QueryParameter.ENDTIMEAT.key to listOf(query.temporalQ?.endTimeAt),
        QueryParameter.AGGRPERIODDURATION.key to listOf(query.temporalQ?.aggrPeriodDuration),
        QueryParameter.AGGRMETHODS.key to query.temporalQ?.aggrMethods,
        QueryParameter.LASTN.key to listOf(query.temporalQ?.lastN.toString()),
        QueryParameter.TIMEPROPERTY.key to listOf(query.temporalQ?.timeproperty)
    )
    val temporalQuery = buildTemporalQuery(
        MultiValueMapAdapter(temporalParams),
        defaultPagination,
        true,
        temporalRepresentation
    ).bind()

    TemporalEntitiesQueryFromPost(
        entitiesQuery = entitiesQueryFromPost,
        temporalQuery = temporalQuery,
        temporalRepresentation = temporalRepresentation,
        withAudit = withAudit
    )
}

@SuppressWarnings("ReturnCount")
fun buildTemporalQuery(
    params: MultiValueMap<String, String>,
    pagination: ApplicationProperties.Pagination,
    inQueryEntities: Boolean = false,
    temporalRepresentation: TemporalRepresentation,
): Either<APIException, TemporalQuery> {
    val timerelParam = params.getFirst(QueryParameter.TIMEREL.key)
    val timeAtParam = params.getFirst(QueryParameter.TIMEAT.key)
    val endTimeAtParam = params.getFirst(QueryParameter.ENDTIMEAT.key)
    val withAggregatedValues = temporalRepresentation == TemporalRepresentation.AGGREGATED_VALUES
    val aggrPeriodDurationParam =
        if (withAggregatedValues)
            params.getFirst(QueryParameter.AGGRPERIODDURATION.key) ?: WHOLE_TIME_RANGE_DURATION
        else null
    val aggrMethodsParam = params.getFirst(QueryParameter.AGGRMETHODS.key)
    val lastNParam = params.getFirst(QueryParameter.LASTN.key)
    val timeproperty = params.getFirst(QueryParameter.TIMEPROPERTY.key)?.let {
        AttributeInstance.TemporalProperty.forPropertyName(it)
    } ?: AttributeInstance.TemporalProperty.OBSERVED_AT

    val endTimeAt = endTimeAtParam?.parseTimeParameter("'endTimeAt' parameter is not a valid date")
        ?.getOrElse {
            return BadRequestDataException(it).left()
        }

    val (timerel, timeAt) = buildTimerelAndTime(timerelParam, timeAtParam, inQueryEntities).getOrElse {
        return BadRequestDataException(it).left()
    }

    if (timerel == Timerel.BETWEEN && endTimeAtParam == null)
        return BadRequestDataException("'endTimeAt' request parameter is mandatory if 'timerel' is 'between'").left()

    if (withAggregatedValues && aggrMethodsParam == null)
        return BadRequestDataException("'aggrMethods' is mandatory if 'aggregatedValues' option is specified").left()

    val aggregate = aggrMethodsParam?.split(",")?.map {
        if (Aggregate.isSupportedAggregate(it))
            Aggregate.forMethod(it)!!
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
): Either<String, Pair<Timerel?, ZonedDateTime?>> =
    // when querying a specific temporal entity, timeAt and timerel are optional
    if (timerelParam == null && timeAtParam == null && !inQueryEntities) {
        Pair(null, null).right()
    } else if (timerelParam != null && timeAtParam != null) {
        val timeRelResult = try {
            Timerel.valueOf(timerelParam.uppercase()).right()
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

fun extractTemporalRepresentation(
    requestParams: MultiValueMap<String, String>
): Either<APIException, TemporalRepresentation> = either {
    val optionsParam = Optional.ofNullable(requestParams.getFirst(QueryParameter.OPTIONS.key))
    val formatParam = requestParams.getFirst(QueryParameter.FORMAT.key)
    return when (formatParam) {
        FormatValue.TEMPORAL_VALUES.value -> TemporalRepresentation.TEMPORAL_VALUES.right()
        FormatValue.AGGREGATED_VALUES.value -> TemporalRepresentation.AGGREGATED_VALUES.right()
        else -> {
            val hasTemporal = hasValueInQueryParam(optionsParam, QueryParamValue.TEMPORAL_VALUES)
            val hasAggregated = hasValueInQueryParam(optionsParam, QueryParamValue.AGGREGATED_VALUES)
            when {
                hasTemporal && hasAggregated ->
                    return BadRequestDataException("Only one temporal representation can be present").left()
                hasTemporal -> TemporalRepresentation.TEMPORAL_VALUES.right()
                hasAggregated -> TemporalRepresentation.AGGREGATED_VALUES.right()
                else -> TemporalRepresentation.NONE.right()
            }
        }
    }
}

enum class TemporalRepresentation {
    TEMPORAL_VALUES,
    AGGREGATED_VALUES,
    NONE
}
