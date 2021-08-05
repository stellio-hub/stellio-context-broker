package com.egm.stellio.search.web

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.getOrHandle
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.util.JSON_LD_CONTENT_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandValueAsListOfMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.OptionsParamValue
import com.egm.stellio.shared.util.buildGetSuccessResponse
import com.egm.stellio.shared.util.checkAndGetContext
import com.egm.stellio.shared.util.getApplicableMediaType
import com.egm.stellio.shared.util.getContextFromLinkHeaderOrDefault
import com.egm.stellio.shared.util.hasValueInOptionsParam
import com.egm.stellio.shared.util.parseAndExpandRequestParameter
import com.egm.stellio.shared.util.parseTimeParameter
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.net.URI
import java.time.ZonedDateTime
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
class TemporalEntityHandler(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val queryService: QueryService
) {

    /**
     * Implements 6.20.3.1 - Add attributes to Temporal Representation of Entity
     */
    @PostMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addAttributes(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val body = requestBody.awaitFirst()
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)

        jsonLdAttributes
            .forEach { attributeEntry ->
                val attributeInstances = expandValueAsListOfMap(attributeEntry.value)
                attributeInstances.forEach { attributeInstance ->
                    val datasetId = attributeInstance.getDatasetId()
                    val temporalEntityAttributeUuid = temporalEntityAttributeService.getForEntityAndAttribute(
                        entityId.toUri(),
                        attributeEntry.key,
                        datasetId
                    ).awaitFirst()

                    val compactedAttributeName = compactTerm(attributeEntry.key, contexts)
                    attributeInstanceService.addAttributeInstance(
                        temporalEntityAttributeUuid,
                        compactedAttributeName,
                        attributeInstance,
                        contexts
                    ).awaitFirst()
                }
            }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
    }

    /**
     * Partial implementation of 6.18.3.2 - Query Temporal Evolution of Entities
     */
    @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getForEntities(
        @RequestHeader httpHeaders: HttpHeaders,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)
        val parsedParams = queryService.parseAndCheckQueryParams(params, contextLink)

        val temporalEntities = queryService.queryTemporalEntities(
            parsedParams["ids"] as Set<URI>,
            parsedParams["types"] as Set<String>,
            parsedParams["temporalQuery"] as TemporalQuery,
            parsedParams["withTemporalValues"] as Boolean,
            contextLink
        )

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(serializeObject(temporalEntities.map { addContextsToEntity(it, listOf(contextLink), mediaType) }))
    }

    /**
     * Partial implementation of 6.19.3.1 (query parameters are not all supported)
     */
    @GetMapping("/{entityId}", produces = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun getForEntity(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestParam params: MultiValueMap<String, String>
    ): ResponseEntity<*> {
        val withTemporalValues =
            hasValueInOptionsParam(Optional.ofNullable(params.getFirst("options")), OptionsParamValue.TEMPORAL_VALUES)
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders)
        val mediaType = getApplicableMediaType(httpHeaders)

        val temporalQuery = try {
            buildTemporalQuery(params, contextLink)
        } catch (e: BadRequestDataException) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse(e.message))
        }

        val temporalEntity =
            queryService.queryTemporalEntity(entityId.toUri(), temporalQuery, withTemporalValues, contextLink)

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(serializeObject(addContextsToEntity(temporalEntity, listOf(contextLink), mediaType)))
    }
}

internal fun buildTemporalQuery(params: MultiValueMap<String, String>, contextLink: String): TemporalQuery {
    val timerelParam = params.getFirst("timerel")
    val timeParam = params.getFirst("time")
    val endTimeParam = params.getFirst("endTime")
    val timeBucketParam = params.getFirst("timeBucket")
    val aggregateParam = params.getFirst("aggregate")
    val lastNParam = params.getFirst("lastN")
    val attrsParam = params.getFirst("attrs")

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
        lastN = lastN
    )
}

internal fun buildTimerelAndTime(
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
