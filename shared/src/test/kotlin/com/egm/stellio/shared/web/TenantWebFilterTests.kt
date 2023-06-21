package com.egm.stellio.shared.web

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.MockkedHandler
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TenantWebFilterTests {

    private val applicationProperties = ApplicationProperties(
        authentication = ApplicationProperties.Authentication(true),
        pagination = ApplicationProperties.Pagination(100, 30),
        tenants = listOf(
            ApplicationProperties.TenantConfiguration(DEFAULT_TENANT_URI, "http://localhost", "public"),
            ApplicationProperties.TenantConfiguration("urn:ngsi-ld:tenant:01".toUri(), "http://localhost", "tenant_01")
        )
    )

    private lateinit var webClient: WebTestClient

    @BeforeAll
    fun initializeWebFilter() {
        val tenantWebFilter = TenantWebFilter(applicationProperties)
        tenantWebFilter.initializeTenantsUris()
        webClient = WebTestClient.bindToController(MockkedHandler())
            .webFilter<WebTestClient.ControllerSpec>(tenantWebFilter)
            .build()
    }

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
    fun `it should return a NonexistentTenant error if the tenant does not exist`() {
        webClient.get()
            .uri("/router/mockkedroute/ok")
            .header(NGSILD_TENANT_HEADER, "urn:ngsi-ld:tenant:02")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.NOT_FOUND)
            .expectBody()
            .jsonPath("$..type").isEqualTo("https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant")
            .jsonPath("$..detail")
            .isEqualTo("Tenant urn:ngsi-ld:tenant:02 does not exist")
    }

    @Test
    fun `it should find a non-default tenant and add the NGSILD-Tenant header in the response`() {
        webClient.get()
            .uri("/router/mockkedroute/ok")
            .header(NGSILD_TENANT_HEADER, "urn:ngsi-ld:tenant:01")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.OK)
            .expectHeader().exists(NGSILD_TENANT_HEADER)
            .expectHeader().valueEquals(NGSILD_TENANT_HEADER, "urn:ngsi-ld:tenant:01")
    }

    @Test
    fun `it should fallback to the default tenant if none was provided in the request`() {
        webClient.get()
            .uri("/router/mockkedroute/ok")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.OK)
            .expectHeader().doesNotExist(NGSILD_TENANT_HEADER)
    }
}
