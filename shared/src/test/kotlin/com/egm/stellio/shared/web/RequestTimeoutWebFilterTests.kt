package com.egm.stellio.shared.web

import com.egm.stellio.shared.config.ApplicationProperties
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Duration

class RequestTimeoutWebFilterTests {

    private val webClient = buildWebClient(Duration.ofMillis(50))

    @Test
    fun `slow query endpoint should return a gateway timeout`() {
        webClient.get()
            .uri("/slow-query")
            .exchange()
            .expectGatewayTimeout()
    }

    @Test
    fun `slow update endpoint should return a gateway timeout`() {
        webClient.post()
            .uri("/slow-update")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("{}")
            .exchange()
            .expectGatewayTimeout()
    }

    @Test
    fun `negative timeout should disable request timeout`() {
        buildWebClient(Duration.ofMillis(-1))
            .get()
            .uri("/slow-query")
            .exchange()
            .expectStatus().isOk
            .expectBody(String::class.java).isEqualTo("query completed")
    }

    @Test
    fun `zero timeout should disable request timeout`() {
        buildWebClient(Duration.ZERO)
            .get()
            .uri("/slow-query")
            .exchange()
            .expectStatus().isOk
            .expectBody(String::class.java).isEqualTo("query completed")
    }

    private fun WebTestClient.ResponseSpec.expectGatewayTimeout() {
        expectStatus().isEqualTo(504)
            .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
            .expectBody()
            .jsonPath("$.type").isEqualTo("https://uri.etsi.org/ngsi-ld/errors/GatewayTimeout")
            .jsonPath("$.title").isEqualTo("Request timed out")
            .jsonPath("$.detail")
            .isEqualTo("The request exceeded the configured timeout of 50 ms and was cancelled")
    }

    private fun buildWebClient(requestTimeout: Duration): WebTestClient =
        WebTestClient.bindToController(TimeoutTestController())
            .webFilter<WebTestClient.ControllerSpec>(
                RequestTimeoutWebFilter(applicationProperties(requestTimeout))
            )
            .build()

    private fun applicationProperties(requestTimeout: Duration) =
        ApplicationProperties(
            authentication = ApplicationProperties.Authentication(false, emptyList()),
            pagination = ApplicationProperties.Pagination(30, 100, 10_000),
            tenants = emptyList(),
            contexts = ApplicationProperties.Contexts("", "", ""),
            requestTimeout = requestTimeout
        )
}

@RestController
private class TimeoutTestController {

    @GetMapping("/slow-query")
    suspend fun slowQuery(): String {
        delay(1_000)
        return "query completed"
    }

    @PostMapping("/slow-update")
    suspend fun slowUpdate(): String {
        delay(1_000)
        return "update completed"
    }
}
