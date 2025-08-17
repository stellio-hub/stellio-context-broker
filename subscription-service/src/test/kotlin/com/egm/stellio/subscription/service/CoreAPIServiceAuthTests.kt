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
import org.junit.jupiter.api.Assertions.fail
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
        // declare a tenant with issuer and per-tenant client credentials mapped to wiremock
        // need to re-declare all the properties for the tenant (can't override only issuer)
        "application.tenants[0].name=urn:ngsi-ld:tenant:default",
        "application.tenants[0].issuer=http://localhost:8089/realms/default",
        "application.tenants[0].dbSchema=default",
        "application.tenants[0].clientId=stellio-subscription",
        "application.tenants[0].clientSecret=changeit",
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
    fun `it should attach bearer token for tenant when calling entity service`() {
        // stub token endpoint for tenant 01
        stubFor(
            post(urlEqualTo("/realms/default/protocol/openid-connect/token"))
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
                tenantName = "urn:ngsi-ld:tenant:default",
                paramRequest = "?type=BeeHive",
                contextLink = APIC_HEADER_LINK
            )
            assertEquals(0, result.size)
        }

        // verify both token call and entity call with proper headers
        verify(
            postRequestedFor(urlEqualTo("/realms/default/protocol/openid-connect/token"))
                .withRequestBody(containing("grant_type=client_credentials"))
                .withBasicAuth(BasicCredentials("stellio-subscription", "changeit"))
        )
        verify(
            getRequestedFor(urlEqualTo("/ngsi-ld/v1/entities?type=BeeHive"))
                .withHeader(LINK, equalTo(APIC_HEADER_LINK))
                .withHeader(NGSILD_TENANT_HEADER, equalTo("urn:ngsi-ld:tenant:default"))
                .withHeader(AUTHORIZATION, equalTo("Bearer access-token-01"))
        )
    }

    @Test
    fun `it should raise error when tenant is unknown`() {
        // stub entity service endpoint (but no token endpoint for unknown tenant)
        stubFor(
            get(urlEqualTo("/ngsi-ld/v1/entities?type=BeeHive"))
                .willReturn(
                    okJson("[]")
                )
        )

        runBlocking {
            try {
                coreApiService.getEntities(
                    tenantName = "urn:ngsi-ld:tenant:unknown",
                    paramRequest = "?type=BeeHive",
                    contextLink = APIC_HEADER_LINK
                )
                // Should not reach here - expect an exception
                fail("Expected an exception for unknown tenant")
            } catch (e: Exception) {
                // Verify that an exception is thrown for unknown tenant
                // The error should be related to client registration not found
                assertEquals(
                    "Could not find ClientRegistration with id 'urn:ngsi-ld:tenant:unknown'",
                    e.message
                )
            }
        }
    }
}
