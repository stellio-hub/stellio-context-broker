package com.egm.stellio.search.util

import arrow.core.*
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import org.springframework.util.MultiValueMap
import java.time.ZonedDateTime
import java.util.*

fun parseAndCheckQueryParams(
    pagination: ApplicationProperties.Pagination,
    queryParams: MultiValueMap<String, String>,
    contextLink: String
): TemporalEntitiesQuery {
    val withTemporalValues = hasValueInOptionsParam(
        Optional.ofNullable(queryParams.getFirst(QUERY_PARAM_OPTIONS)), OptionsParamValue.TEMPORAL_VALUES
    )
    val withAudit = hasValueInOptionsParam(
        Optional.ofNullable(queryParams.getFirst(QUERY_PARAM_OPTIONS)), OptionsParamValue.AUDIT
    )
    val count = queryParams.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
    val ids = parseRequestParameter(queryParams.getFirst(QUERY_PARAM_ID)).map { it.toUri() }.toSet()
    val types = parseAndExpandRequestParameter(queryParams.getFirst(QUERY_PARAM_TYPE), contextLink)
    val temporalQuery = buildTemporalQuery(queryParams, contextLink)
    val (offset, limit) = extractAndValidatePaginationParameters(
        queryParams,
        pagination.limitDefault,
        pagination.limitMax,
        count
    )

    if (types.isEmpty() && temporalQuery.expandedAttrs.isEmpty())
        throw BadRequestDataException("Either type or attrs need to be present in request parameters")

    return TemporalEntitiesQuery(
        ids = ids,
        types = types,
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit,
        limit = limit,
        offset = offset,
        count = count
    )
}

fun buildTemporalQuery(params: MultiValueMap<String, String>, contextLink: String): TemporalQuery {
    val timerelParam = params.getFirst("timerel")
    val timeParam = params.getFirst("time")
    val endTimeParam = params.getFirst("endTime")
    val timeBucketParam = params.getFirst("timeBucket")
    val aggregateParam = params.getFirst("aggregate")
    val lastNParam = params.getFirst("lastN")
    val attrsParam = params.getFirst("attrs")
    val timeproperty = params.getFirst("timeproperty")?.let {
        AttributeInstance.TemporalProperty.forPropertyName(it)
    } ?: AttributeInstance.TemporalProperty.OBSERVED_AT

    if (timerelParam == "between" && endTimeParam == null)
        throw BadRequestDataException("'endTime' request parameter is mandatory if 'timerel' is 'between'")

    val endTime = endTimeParam?.parseTimeParameter("'endTime' parameter is not a valid date")
        ?.getOrHandle {
            throw BadRequestDataException(it)
        }

    val (timerel, time) = buildTimerelAndTime(timerelParam, timeParam).getOrHandle {
        throw BadRequestDataException(it)
    }

    if (listOf(timeBucketParam, aggregateParam).filter { it == null }.size == 1)
        throw BadRequestDataException("'timeBucket' and 'aggregate' must be used in conjunction")

    val aggregate = aggregateParam?.let {
        if (TemporalQuery.Aggregate.isSupportedAggregate(it))
            TemporalQuery.Aggregate.valueOf(it)
        else
            throw BadRequestDataException("Value '$it' is not supported for 'aggregate' parameter")
    }

    val lastN = lastNParam?.toIntOrNull()?.let {
        if (it >= 1) it else null
    }

    val expandedAttrs = parseAndExpandRequestParameter(attrsParam, contextLink)

    return TemporalQuery(
        expandedAttrs = expandedAttrs,
        timerel = timerel,
        time = time,
        endTime = endTime,
        timeBucket = timeBucketParam,
        aggregate = aggregate,
        lastN = lastN,
        timeproperty = timeproperty
    )
}

fun buildTimerelAndTime(
    timerelParam: String?,
    timeParam: String?
): Either<String, Pair<TemporalQuery.Timerel?, ZonedDateTime?>> =
    if (timerelParam == null && timeParam == null) {
        Pair(null, null).right()
    } else if (timerelParam != null && timeParam != null) {
        val timeRelResult = try {
            TemporalQuery.Timerel.valueOf(timerelParam.uppercase()).right()
        } catch (e: IllegalArgumentException) {
            "'timerel' is not valid, it should be one of 'before', 'between', or 'after'".left()
        }

        timeRelResult.flatMap { timerel ->
            timeParam.parseTimeParameter("'time' parameter is not a valid date")
                .map {
                    Pair(timerel, it)
                }
        }
    } else {
        "'timerel' and 'time' must be used in conjunction".left()
    }
