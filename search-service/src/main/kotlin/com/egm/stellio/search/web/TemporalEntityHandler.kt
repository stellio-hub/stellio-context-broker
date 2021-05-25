package com.egm.stellio.search.web

import arrow.core.*
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.TemporalEntityService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandValueAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrDefault
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.util.MultiValueMap
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.*

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
class TemporalEntityHandler(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val entityService: EntityService,
    private val temporalEntityService: TemporalEntityService,
    private val handlerUtils: HandlerUtils
) {

    /**
     * Mirror of what we receive from Kafka.
     *
     * Implements 6.20.3.1
     */
    @PostMapping("/{entityId}/attrs", consumes = [MediaType.APPLICATION_JSON_VALUE, JSON_LD_CONTENT_TYPE])
    suspend fun addAttrs(
        @RequestHeader httpHeaders: HttpHeaders,
        @PathVariable entityId: String,
        @RequestBody requestBody: Mono<String>
    ): ResponseEntity<*> {
        val body = requestBody.awaitFirst()
        val parsedBody = JsonUtils.deserializeObject(body)
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)

        jsonLdAttributes
            .forEach {
                val compactedAttributeName = compactTerm(it.key, contexts)
                val temporalEntityAttributeUuid = temporalEntityAttributeService.getForEntityAndAttribute(
                    entityId.toUri(),
                    it.key
                ).awaitFirst()

                attributeInstanceService.addAttributeInstances(
                    temporalEntityAttributeUuid,
                    compactedAttributeName,
                    expandValueAsMap(it.value),
                    parsedBody
                ).awaitFirst()
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

        val temporalEntities = handlerUtils.queryTemporalEntities(params, contextLink).awaitFirst()

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

        val temporalEntityAttributes = temporalEntityAttributeService.getForEntity(
            entityId.toUri(),
            temporalQuery.expandedAttrs
        ).collectList().awaitFirst().ifEmpty { throw ResourceNotFoundException(entityNotFoundMessage(entityId)) }

        val attributeAndResultsMap = temporalEntityAttributes.map {
            it to attributeInstanceService.search(temporalQuery, it, withTemporalValues).awaitFirst()
        }.toMap()

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            entityId.toUri(),
            attributeAndResultsMap,
            temporalQuery,
            listOf(contextLink),
            withTemporalValues
        )

        return buildGetSuccessResponse(mediaType, contextLink)
            .body(serializeObject(addContextsToEntity(temporalEntity, listOf(contextLink), mediaType)))
    }

    /**
     * Get the entity payload from entity service if we don't have it locally (for legacy entries in DB)
     */
    private fun loadEntityPayload(
        temporalEntityAttribute: TemporalEntityAttribute,
        bearerToken: String,
        contextLink: String
    ): Mono<JsonLdEntity> =
        when {
            temporalEntityAttribute.entityPayload == null ->
                entityService.getEntityById(temporalEntityAttribute.entityId, bearerToken)
                    .doOnSuccess {
                        val entityPayload = compact(it, contextLink)
                        temporalEntityAttributeService.updateEntityPayload(
                            temporalEntityAttribute.entityId,
                            serializeObject(entityPayload)
                        ).subscribe()
                    }
            temporalEntityAttribute.type != "https://uri.etsi.org/ngsi-ld/Subscription" -> Mono.just(
                expandJsonLdEntity(
                    temporalEntityAttribute.entityPayload
                )
            )
            else -> {
                val parsedEntity = expandJsonLdEntity(temporalEntityAttribute.entityPayload)
                Mono.just(parsedEntity)
            }
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
            TemporalQuery.Timerel.valueOf(timerelParam.toUpperCase()).right()
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
