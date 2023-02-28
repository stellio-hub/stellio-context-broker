package com.egm.stellio.search.util

import arrow.core.*
import arrow.core.continuations.either
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import org.springframework.util.MultiValueMap
import java.time.ZonedDateTime
import java.util.Optional

suspend fun parseQueryAndTemporalParams(
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

    if (queryParams.types.isEmpty() && queryParams.attrs.isEmpty() && inQueryEntities)
        BadRequestDataException("Either type or attrs need to be present in request parameters").left().bind()

    val withTemporalValues = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.TEMPORAL_VALUES
    )
    val withAudit = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)),
        OptionsParamValue.AUDIT
    )
    val temporalQuery = buildTemporalQuery(requestParams, inQueryEntities).bind()

    TemporalEntitiesQuery(
        queryParams = queryParams,
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit
    )
}

fun buildTemporalQuery(
    params: MultiValueMap<String, String>,
    inQueryEntities: Boolean = false
): Either<APIException, TemporalQuery> {
    val timerelParam = params.getFirst("timerel")
    val timeAtParam = params.getFirst("timeAt")
    val endTimeAtParam = params.getFirst("endTimeAt")
    val timeBucketParam = params.getFirst("timeBucket")
    val aggregateParam = params.getFirst("aggregate")
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

    if (listOf(timeBucketParam, aggregateParam).filter { it == null }.size == 1)
        return BadRequestDataException("'timeBucket' and 'aggregate' must be used in conjunction").left()

    val aggregate = aggregateParam?.let {
        if (TemporalQuery.Aggregate.isSupportedAggregate(it))
            TemporalQuery.Aggregate.valueOf(it)
        else
            throw BadRequestDataException("Value '$it' is not supported for 'aggregate' parameter")
    }

    val lastN = lastNParam?.toIntOrNull()?.let {
        if (it >= 1) it else null
    }

    return TemporalQuery(
        timerel = timerel,
        timeAt = timeAt,
        endTimeAt = endTimeAt,
        timeBucket = timeBucketParam,
        aggregate = aggregate,
        lastN = lastN,
        timeproperty = timeproperty
    ).right()
}

fun buildTimerelAndTime(
    timerelParam: String?,
    timeAtParam: String?,
    inQueryEntities: Boolean
): Either<String, Pair<TemporalQuery.Timerel?, ZonedDateTime?>> =
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
