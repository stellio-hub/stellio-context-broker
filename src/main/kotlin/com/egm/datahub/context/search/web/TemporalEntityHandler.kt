package com.egm.datahub.context.search.web

import com.egm.datahub.context.search.exception.InvalidNgsiLdPayloadException
import com.egm.datahub.context.search.model.TemporalQuery
import com.egm.datahub.context.search.service.NgsiLdParsingService
import com.egm.datahub.context.search.service.ObservationService
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.body
import reactor.core.publisher.Mono
import java.lang.IllegalArgumentException
import java.lang.RuntimeException
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

@Component
class TemporalEntityHandler(
    private val observationService: ObservationService,
    private val ngsiLdParsingService: NgsiLdParsingService
) {

    /**
     * Mirror of what we receive from Kafka.
     *
     * Implements 6.20.3.1
     */
    fun addAttrs(req: ServerRequest): Mono<ServerResponse> {
        return req.bodyToMono(String::class.java)
                .map {
                    ngsiLdParsingService.parseTemporalPropertyUpdate(it)
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
        return Mono.just("")
            .map {
                buildTemporalQuery(req.queryParams(), req.pathVariable("entityId"))
            }.map {
                // TODO : a quick and dirty fix to propagate the Bearer token when calling context registry
                //        there should be a way to do it more transparently
                val bearerToken =
                    if (req.headers().asHttpHeaders().containsKey("Authorization"))
                        req.headers().header("Authorization").first()
                    else
                        ""
                observationService.search(it, bearerToken)
            }.flatMap {
                ok().body(it)
            }.onErrorResume {
                when (it) {
                    is BadQueryParametersException -> badRequest().body(BodyInserters.fromValue(it.message.orEmpty()))
                    else -> status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromValue(it.message.orEmpty()))
                }
            }
    }

    class BadQueryParametersException(message: String) : RuntimeException(message)

    private fun buildTemporalQuery(params: MultiValueMap<String, String>, entityId: String): TemporalQuery {
        if (!params.containsKey("timerel") || !params.containsKey("time"))
            throw BadQueryParametersException("'timerel and 'time' request parameters are mandatory")

        if (params.getFirst("timerel") == "between" && !params.containsKey("endTime"))
            throw BadQueryParametersException("'endTime' request parameter is mandatory if 'timerel' is 'between'")

        val timerel = try {
            TemporalQuery.Timerel.valueOf(params.getFirst("timerel")!!.toUpperCase())
        } catch (e: IllegalArgumentException) {
            throw BadQueryParametersException("'timerel' is not valid, it should be one of 'before', 'between', or 'after'")
        }
        val time = parseTimeParameter(params.getFirst("time")!!, "'time' parameter is not a valid date")
        val endTime = params.getFirst("endTime")?.let { parseTimeParameter(it, "'endTime' parameter is not a valid date") }

        return TemporalQuery(timerel, time, endTime, entityId)
    }

    private fun parseTimeParameter(param: String, errorMsg: String) =
        try {
            OffsetDateTime.parse(param)
        } catch (e: DateTimeParseException) {
            throw BadQueryParametersException(errorMsg)
        }
}
