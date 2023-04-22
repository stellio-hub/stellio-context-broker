package com.egm.stellio.shared.web

import com.egm.stellio.shared.util.MockkedHandler
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
class TenantWebFilterTests {
    private val webClient = WebTestClient.bindToController(MockkedHandler())
        .webFilter<WebTestClient.ControllerSpec>(TenantWebFilter())
        .build()

    @Test
    fun `it should return a BadRequestData error if the tenant is not a valid URI`() {
        webClient.get()
            .uri("/router/mockkedroute/ok")
            .header(NGSILD_TENANT_HEADER, "not-an-uri")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST)
            .expectBody()
            .jsonPath("$..type").isEqualTo("https://uri.etsi.org/ngsi-ld/errors/BadRequestData")
            .jsonPath("$..detail")
            .isEqualTo("The supplied identifier was expected to be an URI but it is not: not-an-uri (tenant)")
    }

    @Test
    fun `it should add the NGSILD-Tenant header in responses`() {
        webClient.get()
            .uri("/router/mockkedroute/ok")
            .header(NGSILD_TENANT_HEADER, "urn:ngsi-ld:tenant:01")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.OK)
            .expectHeader().exists(NGSILD_TENANT_HEADER)
            .expectHeader().valueEquals(NGSILD_TENANT_HEADER, "urn:ngsi-ld:tenant:01")
    }
}
