package com.egm.datahub.context.search.web

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.ok
import reactor.core.publisher.Mono

@Component
class StatusHandler {

    private val logger = LoggerFactory.getLogger(StatusHandler::class.java)

    fun status(req: ServerRequest): Mono<ServerResponse> {
        return ok().build()
    }
}