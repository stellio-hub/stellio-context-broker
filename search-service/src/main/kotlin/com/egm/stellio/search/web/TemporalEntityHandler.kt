package com.egm.stellio.search.web

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ApiUtils.serializeObject
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.NgsiLdParsingUtils.compactEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandValueAsMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseEntity
import com.egm.stellio.shared.util.extractContextFromLinkHeader
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.egm.stellio.shared.util.parseTimeParameter
import org.springframework.stereotype.Component
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.lang.IllegalArgumentException

@Component
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
    fun addAttrs(req: ServerRequest): Mono<ServerResponse> {
        val entityId = req.pathVariable("entityId")
        val contextLink = extractContextFromLinkHeader(req)

        return req.bodyToMono<String>()
            .flatMapMany {
                Flux.fromIterable(expandJsonLdFragment(it, contextLink).asIterable())
            }
            .flatMap {
                temporalEntityAttributeService.getForEntityAndAttribute(entityId, it.key.extractShortTypeFromExpanded())
                    .map { temporalEntityAttributeUuid ->
                        Pair(temporalEntityAttributeUuid, it) }
            }
            .map {
                attributeInstanceService.addAttributeInstances(it.first,
                    it.second.key.extractShortTypeFromExpanded(),
                    expandValueAsMap(it.second.value))
            }
            .collectList()
            .flatMap {
                noContent().build()
            }
    }

    /**
     * Partial implementation of 6.19.3.1 (query parameters are not all supported)
     */
    fun getForEntity(req: ServerRequest): Mono<ServerResponse> {
        val entityId = req.pathVariable("entityId")
        // TODO : a quick and dirty fix to propagate the Bearer token when calling context registry
        //        there should be a way to do it more transparently
        val bearerToken =
            if (req.headers().asHttpHeaders().containsKey("Authorization"))
                req.headers().header("Authorization").first()
            else
                ""

        val temporalQuery = try {
            buildTemporalQuery(req.queryParams())
        } catch (e: BadRequestDataException) {
            return badRequest().body(BodyInserters.fromValue(e.message.orEmpty()))
        }

        // FIXME this is way too complex, refactor it later
        return temporalEntityAttributeService.getForEntity(entityId, temporalQuery.attrs)
            .switchIfEmpty(Flux.error(ResourceNotFoundException("Entity $entityId was not found")))
            .flatMap { temporalEntityAttribute ->
                attributeInstanceService.search(temporalQuery, temporalEntityAttribute)
                    .map { results ->
                        Pair(temporalEntityAttribute, results)
                    }
            }
            .collectList()
            .zipWhen {
                loadEntityPayload(it[0].first, bearerToken)
            }
            .map {
                val listOfResults = it.t1.map {
                    it.second
                }
                temporalEntityAttributeService.injectTemporalValues(it.t2, listOfResults)
            }
            .map {
                compactEntity(it)
            }
            .flatMap {
                ok().body(BodyInserters.fromValue(serializeObject(it)))
            }
    }

    /**
     * Get the entity payload from entity service if we don't have it locally (for legacy entries in DB)
     */
    private fun loadEntityPayload(temporalEntityAttribute: TemporalEntityAttribute, bearerToken: String): Mono<Pair<Map<String, Any>, List<String>>> =
        when {
            temporalEntityAttribute.entityPayload == null ->
                entityService.getEntityById(temporalEntityAttribute.entityId, bearerToken)
                    .doOnSuccess {
                        val entityPayload = compactEntity(it)
                        temporalEntityAttributeService.addEntityPayload(temporalEntityAttribute, serializeObject(entityPayload)).subscribe()
                    }
            temporalEntityAttribute.type != "Subscription" -> Mono.just(parseEntity(temporalEntityAttribute.entityPayload))
            else -> {
                val parsedEntity = parseEntity(temporalEntityAttribute.entityPayload, emptyList())
                Mono.just(Pair(parsedEntity.first, listOf(NGSILD_CORE_CONTEXT)))
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
        (!params.containsKey("timeBucket") && params.containsKey("aggregate")))
        throw BadRequestDataException("'timeBucket' and 'aggregate' must both be provided for aggregated queries")

    val aggregate =
        if (params.containsKey("aggregate"))
            if (TemporalQuery.Aggregate.isSupportedAggregate(params.getFirst("aggregate")!!))
                TemporalQuery.Aggregate.valueOf(params.getFirst("aggregate")!!)
            else
                throw BadRequestDataException("Value '${params.getFirst("aggregate")!!}' is not supported for 'aggregate' parameter")
        else
            null

    return TemporalQuery(params["attrs"].orEmpty(), timerel, time, endTime, params.getFirst("timeBucket"), aggregate)
}
