package com.egm.stellio.search.web

import com.egm.stellio.search.exception.InvalidNgsiLdPayloadException
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.ContextRegistryService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.util.NgsiLdParsingUtils
import com.egm.stellio.search.service.ObservationService
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import reactor.core.publisher.Mono
import java.lang.IllegalArgumentException
import java.lang.RuntimeException
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

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
                    NgsiLdParsingUtils.parseTemporalPropertyUpdate(it)
                }
                .flatMap {
                    observationService.create(it)
                }
                .flatMap {
                    noContent().build()
                }.onErrorResume {
                    when (it) {
                        is InvalidNgsiLdPayloadException -> badRequest().body(BodyInserters.fromValue(it.message.orEmpty()))
                        else -> status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromValue(it.message.orEmpty()))
                    }
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
        } catch (e: BadQueryParametersException) {
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
            }.onErrorResume {
                when (it) {
                    is BadQueryParametersException -> badRequest().body(BodyInserters.fromValue(it.message.orEmpty()))
                    else -> status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromValue(it.message.orEmpty()))
                }
            }
    }

    class BadQueryParametersException(message: String) : RuntimeException(message)
}

internal fun buildTemporalQuery(params: MultiValueMap<String, String>): TemporalQuery {
    if (!params.containsKey("timerel") || !params.containsKey("time"))
        throw TemporalEntityHandler.BadQueryParametersException("'timerel and 'time' request parameters are mandatory")

    if (params.getFirst("timerel") == "between" && !params.containsKey("endTime"))
        throw TemporalEntityHandler.BadQueryParametersException("'endTime' request parameter is mandatory if 'timerel' is 'between'")

    val timerel = try {
        TemporalQuery.Timerel.valueOf(params.getFirst("timerel")!!.toUpperCase())
    } catch (e: IllegalArgumentException) {
        throw TemporalEntityHandler.BadQueryParametersException("'timerel' is not valid, it should be one of 'before', 'between', or 'after'")
    }
    val time = parseTimeParameter(params.getFirst("time")!!, "'time' parameter is not a valid date")
    val endTime = params.getFirst("endTime")?.let { parseTimeParameter(it, "'endTime' parameter is not a valid date") }

    return TemporalQuery(params["attrs"].orEmpty(), timerel, time, endTime)
}

private fun parseTimeParameter(param: String, errorMsg: String) =
    try {
        OffsetDateTime.parse(param)
    } catch (e: DateTimeParseException) {
        throw TemporalEntityHandler.BadQueryParametersException(errorMsg)
    }
