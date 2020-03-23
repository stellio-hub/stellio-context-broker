package com.egm.stellio.search.web

import com.egm.stellio.shared.model.InvalidNgsiLdPayloadException
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.ContextRegistryService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.ObservationService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseTemporalPropertyUpdate
import com.egm.stellio.shared.util.parseTimeParameter
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import org.springframework.stereotype.Component
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import reactor.core.publisher.Mono
import java.lang.IllegalArgumentException

@Component
class TemporalEntityHandler(
    private val observationService: ObservationService,
    private val entityService: EntityService,
    private val contextRegistryService: ContextRegistryService
) {

    /**
     * Mirror of what we receive from Kafka.
     *
     * Implements 6.20.3.1
     */
    fun addAttrs(req: ServerRequest): Mono<ServerResponse> {
        return req.bodyToMono(String::class.java)
                .map {
                    parseTemporalPropertyUpdate(it)
                        ?: throw InvalidNgsiLdPayloadException("Received content misses one or more required attributes")
                }
                .flatMap {
                    observationService.create(it)
                }
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

        return Mono.just(temporalQuery)
            .flatMapMany {
                entityService.getForEntity(entityId, temporalQuery.attrs).map { entityTemporalProperty ->
                    Pair(entityTemporalProperty, it)
                }
            }
            .flatMap {
                observationService.search(it.second, it.first)
            }
            .collectList()
            .zipWith(contextRegistryService.getEntityById(entityId, bearerToken))
            .map {
                entityService.injectTemporalValues(it.t2, it.t1)
            }
            .map {
                JsonLdProcessor.compact(it.first, mapOf("@context" to it.second), JsonLdOptions())
            }
            .flatMap {
                ok().body(BodyInserters.fromValue(it))
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
