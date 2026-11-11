package com.egm.stellio.search.entity.web

import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.entity.service.EntityEventService.Companion.EVENTS_MAY_BE_LOST_FLAG
import com.egm.stellio.shared.util.ErrorMessages.Kafka.ENTITY_EVENTS_MAY_BE_LOST_WARNING
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Adds an NGSILD-Warning to the successful responses of the requests whose entity events were produced while Kafka
 * was unavailable (only possible when `search.on-kafka-unavailable` is `warn`).
 */
@Component
class EntityEventsWarningWebFilter : WebFilter {

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val eventsMayBeLost = AtomicBoolean(false)
        exchange.response.beforeCommit {
            if (eventsMayBeLost.get() && exchange.response.statusCode?.is2xxSuccessful == true)
                exchange.response.headers.add(NGSILDWarning.HEADER_NAME, WARNING.getHeaderMessage())
            Mono.empty()
        }
        return chain.filter(exchange)
            .contextWrite { it.put(EVENTS_MAY_BE_LOST_FLAG, eventsMayBeLost) }
    }

    companion object {
        private val WARNING = NGSILDWarning(
            NGSILDWarning.MISCELLANEOUS_WARNING_CODE,
            ENTITY_EVENTS_MAY_BE_LOST_WARNING,
            "stellio"
        )
    }
}
