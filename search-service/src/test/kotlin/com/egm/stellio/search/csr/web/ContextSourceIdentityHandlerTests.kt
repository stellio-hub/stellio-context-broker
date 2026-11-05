package com.egm.stellio.search.csr.web

import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.NGSILD_TENANT_HEADER
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
@AutoConfigureWebTestClient(timeout = "30000")
@WebFluxTest(ContextSourceIdentityHandler::class)
@TestPropertySource(
    properties = [
        "application.tenants[0].name = urn:ngsi-ld:tenant:default",
        "application.tenants[0].dbSchema = public",
        "application.tenants[1].name = urn:ngsi-ld:tenant:city",
        "application.tenants[1].dbSchema = city",
        "application.context-source-extras.deployment = test",
        "application.context-source-extras.contact.email = ops@example.org"
    ]
)
class ContextSourceIdentityHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    private val sourceIdentityUri = "/ngsi-ld/v1/info/sourceIdentity"

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .build()
    }

    @Test
    fun `retrieve source identity should return 200 and the identity as JSON-LD`() {
        webClient.get()
            .uri(sourceIdentityUri)
            .accept(JSON_LD_MEDIA_TYPE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().contentType(JSON_LD_MEDIA_TYPE)
            .expectBody()
            .jsonPath("$.id").isEqualTo("urn:ngsi-ld:ContextSource:stellio")
            .jsonPath("$.type").isEqualTo("ContextSourceIdentity")
            .jsonPath("$.contextSourceAlias").isEqualTo("stellio")
            .jsonPath("$.contextSourceUptime").isNotEmpty
            .jsonPath("$.contextSourceTimeAt").isNotEmpty
            .jsonPath("$.contextSourceExtras.deployment").isEqualTo("test")
            .jsonPath("$.contextSourceExtras.contact.email").isEqualTo("ops@example.org")
            .jsonPath("$.@context").exists()
    }

    @Test
    fun `retrieve source identity should return the context in a Link header when JSON is requested`() {
        webClient.get()
            .uri(sourceIdentityUri)
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk
            .expectHeader().contentType(MediaType.APPLICATION_JSON)
            .expectHeader().exists(HttpHeaders.LINK)
            .expectBody()
            .jsonPath("$.contextSourceAlias").isEqualTo("stellio")
            .jsonPath("$.@context").doesNotExist()
    }

    @Test
    fun `retrieve source identity should return the identity of the tenant when a tenant is requested`() {
        webClient.get()
            .uri(sourceIdentityUri)
            .header(NGSILD_TENANT_HEADER, "urn:ngsi-ld:tenant:city")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.id").isEqualTo("urn:ngsi-ld:ContextSource:stellio-city")
            .jsonPath("$.contextSourceAlias").isEqualTo("stellio-city")
    }

    @Test
    fun `retrieve source identity should return 204 without body on a HEAD request`() {
        webClient.head()
            .uri(sourceIdentityUri)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty
    }

    @Test
    fun `retrieve source identity should return 400 when a query parameter is provided`() {
        webClient.get()
            .uri("$sourceIdentityUri?limit=10")
            .exchange()
            .expectStatus().isBadRequest
    }
}
