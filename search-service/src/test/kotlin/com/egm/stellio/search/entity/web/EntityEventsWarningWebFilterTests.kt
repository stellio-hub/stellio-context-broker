package com.egm.stellio.search.entity.web

import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.entity.service.EntityEventService.Companion.EVENTS_MAY_BE_LOST_FLAG
import com.egm.stellio.shared.util.ErrorMessages.Kafka.ENTITY_EVENTS_MAY_BE_LOST_WARNING
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.mock.http.server.reactive.MockServerHttpRequest
import org.springframework.mock.web.server.MockServerWebExchange
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicBoolean

class EntityEventsWarningWebFilterTests {

    private val filter = EntityEventsWarningWebFilter()

    @Test
    fun `filter should add an NGSILD-Warning to a successful response whose events may be lost`() {
        val exchange = filterRequest(eventsMayBeLost = true, status = HttpStatus.CREATED)

        assertThat(exchange.response.headers[NGSILDWarning.HEADER_NAME])
            .containsExactly("199 stellio \"$ENTITY_EVENTS_MAY_BE_LOST_WARNING\"")
    }

    @Test
    fun `filter should not add an NGSILD-Warning to a failed response whose events may be lost`() {
        val exchange = filterRequest(eventsMayBeLost = true, status = HttpStatus.BAD_REQUEST)

        assertThat(exchange.response.headers[NGSILDWarning.HEADER_NAME]).isNull()
    }

    @Test
    fun `filter should not add an NGSILD-Warning when no event may be lost`() {
        val exchange = filterRequest(eventsMayBeLost = false, status = HttpStatus.CREATED)

        assertThat(exchange.response.headers[NGSILDWarning.HEADER_NAME]).isNull()
    }

    private fun filterRequest(eventsMayBeLost: Boolean, status: HttpStatus): MockServerWebExchange {
        val exchange = MockServerWebExchange.from(MockServerHttpRequest.post("/ngsi-ld/v1/entities"))
        val chain = WebFilterChain { filteredExchange ->
            Mono.deferContextual { context ->
                if (eventsMayBeLost) context.get<AtomicBoolean>(EVENTS_MAY_BE_LOST_FLAG).set(true)
                filteredExchange.response.statusCode = status
                filteredExchange.response.setComplete()
            }
        }
        filter.filter(exchange, chain).block()
        return exchange
    }
}
