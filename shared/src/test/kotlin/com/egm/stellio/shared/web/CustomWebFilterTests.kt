package com.egm.stellio.shared.web

import com.egm.stellio.shared.util.MockkedHandler
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
class CustomWebFilterTests {
    private val webClient = WebTestClient.bindToController(MockkedHandler()).webFilter<WebTestClient.ControllerSpec>(
        CustomWebFilter()
    ).build()

    @Test
    fun `HEAD request on a GET resource should return 204 without a body nor a Content-Length`() {
        webClient.head()
            .uri("/router/mockkedroute/with-body")
            .exchange()
            .expectStatus().isNoContent
            .expectHeader().doesNotExist(HttpHeaders.CONTENT_LENGTH)
            .expectBody().isEmpty
    }

    @Test
    fun `HEAD request should keep the error status returned by the GET handler`() {
        webClient.head()
            .uri("/router/mockkedroute/not-found")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.NOT_FOUND)
    }

    @Test
    fun `GET request should still return 200 with its body`() {
        webClient.get()
            .uri("/router/mockkedroute/with-body")
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$.id").isEqualTo("urn:ngsi-ld:A:1")
    }
}
