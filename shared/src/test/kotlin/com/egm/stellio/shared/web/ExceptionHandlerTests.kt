package com.egm.stellio.shared.web

import com.egm.stellio.shared.util.MockkedHandler
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
class ExceptionHandlerTests {
    private val webClient = WebTestClient.bindToController(MockkedHandler()).webFilter<WebTestClient.ControllerSpec>(
        CustomWebFilter()
    ).build()

    @Test
    fun `it should raise an error of type InvalidRequest if the request payload is not a valid JSON fragment`() {
        val invalidJsonLdPayload =
            """
            {
               "id":"urn:ngsi-ld:Apiary:XYZ01",,
               "type":"Apiary",
               "@context":[
                  "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
               ]
            }
            """.trimIndent()

        webClient.post()
            .uri("/router/mockkedroute/validate-json-ld-fragment")
            .bodyValue(invalidJsonLdPayload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST)
            .expectBody()
            .jsonPath("$..type").isEqualTo("https://uri.etsi.org/ngsi-ld/errors/InvalidRequest")
    }

    @Test
    fun `it should raise an error of type LdContextNotAvailable if the context is not resolvable`() {
        val invalidJsonPayload =
            """
            {
               "id":"urn:ngsi-ld:Apiary:XYZ01",
               "type":"Apiary",
               "@context":[
                  "unknownContext"
               ]
            }
            """.trimIndent()

        webClient.post()
            .uri("/router/mockkedroute/validate-json-ld-fragment")
            .bodyValue(invalidJsonPayload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
            .expectBody()
            .jsonPath("$..type")
            .isEqualTo("https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable")
    }
}
