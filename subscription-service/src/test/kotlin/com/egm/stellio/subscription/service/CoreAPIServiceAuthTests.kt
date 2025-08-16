package com.egm.stellio.subscription.service

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.config.WebClientConfig
import com.github.tomakehurst.wiremock.client.BasicCredentials
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.post
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.reset
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders.AUTHORIZATION
import org.springframework.http.HttpHeaders.LINK
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [CoreAPIService::class])
@EnableConfigurationProperties(ApplicationProperties::class)
@WireMockTest(httpPort = 8089)
@Import(WebClientConfig::class)
@ActiveProfiles("test")
@TestPropertySource(
    properties = [
        "application.authentication.enabled=true",
        // point entity service to wiremock
        "subscription.entity-service-url=http://localhost:8089",
        // declare a tenant with issuer and per-tenant client credentials mapped to wiremock
        "application.tenants[0].name=urn:ngsi-ld:tenant:01",
        "application.tenants[0].issuer=http://localhost:8089/realms/tenant-01",
        "application.tenants[0].dbSchema=tenant01",
        "application.tenants[0].clientId=test-client-id-01",
        "application.tenants[0].clientSecret=test-client-secret-01",
    ]
)
class CoreAPIServiceAuthTests {

    @Autowired
    private lateinit var coreApiService: CoreAPIService

    @BeforeEach
    fun setup() {
        reset()
    }

    @Test
    fun `it should attach bearer token for tenant 01 when calling entity service`() {
        // stub token endpoint for tenant 01
        stubFor(
            post(urlEqualTo("/realms/tenant-01/protocol/openid-connect/token"))
                .willReturn(
                    aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("""{"access_token":"access-token-01","token_type":"Bearer","expires_in":300}""")
                )
        )
        // stub entity service endpoint
        stubFor(
            get(urlEqualTo("/ngsi-ld/v1/entities?type=BeeHive"))
                .willReturn(
                    okJson("[]")
                )
        )

        runBlocking {
            val result = coreApiService.getEntities(
                tenantName = "urn:ngsi-ld:tenant:01",
                paramRequest = "?type=BeeHive",
                contextLink = APIC_HEADER_LINK
            )
            assertEquals(0, result.size)
        }

        // verify both token call and entity call with proper headers
        verify(
            postRequestedFor(urlEqualTo("/realms/tenant-01/protocol/openid-connect/token"))
                .withRequestBody(containing("grant_type=client_credentials"))
                .withBasicAuth(BasicCredentials("test-client-id-01", "test-client-secret-01"))
        )
        verify(
            getRequestedFor(urlEqualTo("/ngsi-ld/v1/entities?type=BeeHive"))
                .withHeader(LINK, equalTo(APIC_HEADER_LINK))
                .withHeader(NGSILD_TENANT_HEADER, equalTo("urn:ngsi-ld:tenant:01"))
                .withHeader(AUTHORIZATION, equalTo("Bearer access-token-01"))
        )
    }
}
