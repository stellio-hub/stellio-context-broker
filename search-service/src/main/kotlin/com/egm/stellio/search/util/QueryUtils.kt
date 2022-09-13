package com.egm.stellio.search.util

import arrow.core.*
import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import org.springframework.util.MultiValueMap
import java.time.ZonedDateTime
import java.util.Optional

fun parseAndCheckQueryParams(
    pagination: ApplicationProperties.Pagination,
    requestParams: MultiValueMap<String, String>,
    contextLink: String,
    inQueryEntities: Boolean = false
): TemporalEntitiesQuery {
    val queryParams = parseAndCheckParams(
        Pair(pagination.limitDefault, pagination.limitMax),
        requestParams,
        contextLink
    )
    if (queryParams.types.isEmpty() && queryParams.attrs.isEmpty() && inQueryEntities)
        throw BadRequestDataException("Either type or attrs need to be present in request parameters")

    val withTemporalValues = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)), OptionsParamValue.TEMPORAL_VALUES
    )
    val withAudit = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QUERY_PARAM_OPTIONS)), OptionsParamValue.AUDIT
    )
    val temporalQuery = buildTemporalQuery(requestParams, inQueryEntities)

    return TemporalEntitiesQuery(
        queryParams = queryParams,
        temporalQuery = temporalQuery,
        withTemporalValues = withTemporalValues,
        withAudit = withAudit
    )
}

fun buildTemporalQuery(params: MultiValueMap<String, String>, inQueryEntities: Boolean = false): TemporalQuery {
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
        throw BadRequestDataException("'endTimeAt' request parameter is mandatory if 'timerel' is 'between'")

    val endTimeAt = endTimeAtParam?.parseTimeParameter("'endTimeAt' parameter is not a valid date")
        ?.getOrHandle {
            throw BadRequestDataException(it)
        }

    val (timerel, timeAt) = buildTimerelAndTime(timerelParam, timeAtParam, inQueryEntities).getOrHandle {
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

    return TemporalQuery(
        timerel = timerel,
        timeAt = timeAt,
        endTimeAt = endTimeAt,
        timeBucket = timeBucketParam,
        aggregate = aggregate,
        lastN = lastN,
        timeproperty = timeproperty
    )
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

fun Map<String, Any>.addSpecificAccessPolicy(
    specificAccessPolicy: SpecificAccessPolicy?
): Map<String, Any> =
    if (specificAccessPolicy!=null)
        this.plus(AUTH_PROP_SAP to specificAccessPolicy)
    else this
