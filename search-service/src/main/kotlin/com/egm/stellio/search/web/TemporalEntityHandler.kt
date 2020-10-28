package com.egm.stellio.search.web

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandValueAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
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
import java.util.Optional

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
class TemporalEntityHandler(
    private val attributeInstanceService: AttributeInstanceService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val entityService: EntityService
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
        val contexts = checkAndGetContext(httpHeaders, body)
        val jsonLdAttributes = expandJsonLdFragment(body, contexts)

        jsonLdAttributes
            .forEach {
                val temporalEntityAttributeUuid = temporalEntityAttributeService.getForEntityAndAttribute(
                    entityId.toUri(),
                    it.key.extractShortTypeFromExpanded()
                ).awaitFirst()

                attributeInstanceService.addAttributeInstances(
                    temporalEntityAttributeUuid,
                    it.key.extractShortTypeFromExpanded(),
                    expandValueAsMap(it.value)
                ).awaitFirst()
            }
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
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
        val contextLink = getContextFromLinkHeaderOrDefault(httpHeaders.getOrEmpty("Link"))

        // TODO : a quick and dirty fix to propagate the Bearer token when calling context registry
        //        there should be a way to do it more transparently
        val bearerToken = httpHeaders.getOrEmpty("Authorization").firstOrNull() ?: ""

        val temporalQuery = try {
            buildTemporalQuery(params)
        } catch (e: BadRequestDataException) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON)
                .body(BadRequestDataResponse(e.message))
        }

        // TODO : REFACTOR getForEntity retrieves entity payload for each temporalEntityAttribute,it should be done once
        val temporalEntityAttributes = temporalEntityAttributeService.getForEntity(
            entityId.toUri(),
            temporalQuery.attrs,
            contextLink
        ).collectList().awaitFirst().ifEmpty { throw ResourceNotFoundException(entityNotFoundMessage(entityId)) }

        val attributeAndResultsMap = temporalEntityAttributes.map {
            it to attributeInstanceService.search(temporalQuery, it).awaitFirst()
        }.toMap()

        val jsonLdEntity = loadEntityPayload(attributeAndResultsMap.keys.first(), bearerToken).awaitFirst()
        val jsonLdEntityWithTemporalValues = temporalEntityAttributeService.injectTemporalValues(
            jsonLdEntity,
            attributeAndResultsMap.values.toList(),
            withTemporalValues
        )

        val compactedJsonLdEntity = JsonLdUtils.filterCompactedEntityOnAttributes(
            jsonLdEntityWithTemporalValues.compact(),
            temporalQuery.attrs
        )
        return ResponseEntity.status(HttpStatus.OK).body(serializeObject(compactedJsonLdEntity))
    }

    /**
     * Get the entity payload from entity service if we don't have it locally (for legacy entries in DB)
     */
    private fun loadEntityPayload(
        temporalEntityAttribute: TemporalEntityAttribute,
        bearerToken: String
    ): Mono<JsonLdEntity> =
        when {
            temporalEntityAttribute.entityPayload == null ->
                entityService.getEntityById(temporalEntityAttribute.entityId, bearerToken)
                    .doOnSuccess {
                        val entityPayload = it.compact()
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

internal fun buildTemporalQuery(params: MultiValueMap<String, String>): TemporalQuery {
    if (!params.containsKey("timerel") || !params.containsKey("time"))
        throw BadRequestDataException("'timerel and 'time' request parameters are mandatory")

    if (params.getFirst("timerel") == "between" && !params.containsKey("endTime"))
        throw BadRequestDataException("'endTime' request parameter is mandatory if 'timerel' is 'between'")

    val timerel = try {
        TemporalQuery.Timerel.valueOf(params.getFirst("timerel")!!.toUpperCase())
    } catch (e: IllegalArgumentException) {
        throw BadRequestDataException("'timerel' is not valid, it should be one of 'before', 'between', or 'after'")
    }
    val time = params.getFirst("time")!!.parseTimeParameter("'time' parameter is not a valid date")
    val endTime = params.getFirst("endTime")?.parseTimeParameter("'endTime' parameter is not a valid date")

    if ((params.containsKey("timeBucket") && !params.containsKey("aggregate")) ||
        (!params.containsKey("timeBucket") && params.containsKey("aggregate"))
    )
        throw BadRequestDataException("'timeBucket' and 'aggregate' must both be provided for aggregated queries")

    val aggregate =
        if (params.containsKey("aggregate"))
            if (TemporalQuery.Aggregate.isSupportedAggregate(params.getFirst("aggregate")!!))
                TemporalQuery.Aggregate.valueOf(params.getFirst("aggregate")!!)
            else
                throw BadRequestDataException(
                    "Value '${params.getFirst("aggregate")!!}' is not supported for 'aggregate' parameter"
                )
        else
            null

    val lastN = params.getFirst("lastN")?.toIntOrNull()?.let {
        if (it >= 1) it else null
    }

    return TemporalQuery(
        attrs = params.getFirst("attrs")?.split(",")?.toSet().orEmpty(),
        timerel = timerel,
        time = time,
        endTime = endTime,
        timeBucket = params.getFirst("timeBucket"),
        aggregate = aggregate,
        lastN = lastN
    )
}
