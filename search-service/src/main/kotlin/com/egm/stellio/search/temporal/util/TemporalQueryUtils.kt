package com.egm.stellio.search.temporal.util

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.getOrElse
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.model.EntitiesQuery
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
import com.egm.stellio.shared.model.COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.hasValueInOptionsParam
import com.egm.stellio.shared.util.parseTimeParameter
import org.springframework.util.MultiValueMap
import org.springframework.util.MultiValueMapAdapter
import java.time.ZonedDateTime
import java.util.*

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
    if (inQueryEntities)
        entitiesQueryFromGet.validateMinimalQueryEntitiesParameters().bind()
    val temporalRepresentation = extractTemporalRepresentation(requestParams).bind()
    val withAudit = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QueryParameter.OPTIONS.key)),
        OptionsValue.AUDIT
    ).bind()
    val temporalQuery =
        buildTemporalQuery(requestParams, defaultPagination, inQueryEntities, temporalRepresentation).bind()
    val expandedPickOmitAttributes = expandAttributesFromPickOmit(entitiesQueryFromGet, contexts)

    TemporalEntitiesQueryFromGet(
        entitiesQuery = entitiesQueryFromGet,
        expandedPickOmitAttributes = expandedPickOmitAttributes,
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

    val withAudit = hasValueInOptionsParam(
        Optional.ofNullable(requestParams.getFirst(QueryParameter.OPTIONS.key)),
        OptionsValue.AUDIT
    ).bind()
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
    val expandedPickOmitAttributes = expandAttributesFromPickOmit(entitiesQueryFromPost, contexts)

    TemporalEntitiesQueryFromPost(
        entitiesQuery = entitiesQueryFromPost,
        expandedPickOmitAttributes = expandedPickOmitAttributes,
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
        AttributeInstance.TemporalProperty.fromTimeProperty(it).getOrElse { apiException ->
            return apiException.left()
        }
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
        } catch (_: IllegalArgumentException) {
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

private fun expandAttributesFromPickOmit(
    entitiesQueryFromGet: EntitiesQuery,
    contexts: List<String>
): Pair<Set<ExpandedTerm>, Set<ExpandedTerm>> =
    entitiesQueryFromGet.let {
        Pair(
            it.pick.minus(COMPACTED_ENTITY_CORE_MEMBERS),
            it.omit.minus(COMPACTED_ENTITY_CORE_MEMBERS)
        )
    }.let {
        Pair(
            it.first.map { attr -> expandJsonLdTerm(attr, contexts) }.toSet(),
            it.second.map { attr -> expandJsonLdTerm(attr, contexts) }.toSet()
        )
    }

private fun extractTemporalRepresentation(
    queryParams: MultiValueMap<String, String>
): Either<APIException, TemporalRepresentation> = either {
    val optionsParam = Optional.ofNullable(queryParams.getFirst(QueryParameter.OPTIONS.key))
    val formatParam = queryParams.getFirst(QueryParameter.FORMAT.key)
    return if (formatParam != null) {
        TemporalRepresentation.fromString(formatParam)
    } else {
        if (!optionsParam.isEmpty) {
            val hasTemporal = hasValueInOptionsParam(optionsParam, OptionsValue.TEMPORAL_VALUES).bind()
            val hasAggregated = hasValueInOptionsParam(optionsParam, OptionsValue.AGGREGATED_VALUES).bind()
            when {
                hasTemporal && hasAggregated ->
                    BadRequestDataException(
                        "Found different temporal representations in options query parameter," +
                            " only one can be provided"
                    ).left()
                hasTemporal -> TemporalRepresentation.TEMPORAL_VALUES.right()
                hasAggregated -> TemporalRepresentation.AGGREGATED_VALUES.right()
                else -> TemporalRepresentation.NORMALIZED.right()
            }
        } else
            TemporalRepresentation.NORMALIZED.right()
    }
}

enum class TemporalRepresentation(val key: String) {
    TEMPORAL_VALUES("temporalValues"),
    AGGREGATED_VALUES("aggregatedValues"),
    NORMALIZED("normalized");
    companion object {
        fun fromString(key: String): Either<APIException, TemporalRepresentation> = either {
            TemporalRepresentation.entries.find { it.key == key }
                ?: return InvalidRequestException("'$key' is not a valid temporal representation").left()
        }
    }
}
