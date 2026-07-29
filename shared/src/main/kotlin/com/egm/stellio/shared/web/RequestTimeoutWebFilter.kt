package com.egm.stellio.shared.web

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.GatewayTimeoutException
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.http.HttpHeaders.CONTENT_TYPE
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono
import java.util.concurrent.TimeoutException

private const val REQUEST_TIMEOUT_MESSAGE = "Request timed out"

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
class RequestTimeoutWebFilter(
    private val applicationProperties: ApplicationProperties
) : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> =
        chain.filter(exchange)
            .timeout(applicationProperties.requestTimeout)
            .onErrorResume(TimeoutException::class.java) {
                writeTimeoutResponse(exchange)
            }

    private fun writeTimeoutResponse(exchange: ServerWebExchange): Mono<Void> {
        if (exchange.response.isCommitted)
            return exchange.response.setComplete()

        exchange.response.statusCode = HttpStatus.GATEWAY_TIMEOUT
        exchange.response.headers[CONTENT_TYPE] = MediaType.APPLICATION_JSON_VALUE

        val timeout = applicationProperties.requestTimeout
        val errorResponse = serializeObject(
            GatewayTimeoutException(
                REQUEST_TIMEOUT_MESSAGE,
                "The request exceeded the configured timeout of ${timeout.toMillis()} ms and was cancelled"
            ).toProblemDetail()
        )
        val buffer = exchange.response.bufferFactory().wrap(errorResponse.toByteArray())
        return exchange.response.writeWith(Mono.just(buffer))
    }
}
