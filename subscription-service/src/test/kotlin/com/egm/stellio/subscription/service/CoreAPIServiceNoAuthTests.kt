package com.egm.stellio.subscription.service

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.config.WebClientConfig
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
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
        "application.authentication.enabled=false"
    ]
)
class CoreAPIServiceNoAuthTests {

    @Autowired
    private lateinit var coreApiService: CoreAPIService

    @BeforeEach
    fun setup() {
        reset()
    }

    @Test
    fun `it should not attach bearer token when authentication is disabled`() {
        // stub entity service endpoint only (no token endpoint needed)
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

        // verify entity call without authorization header
        verify(
            getRequestedFor(urlEqualTo("/ngsi-ld/v1/entities?type=BeeHive"))
                .withHeader(LINK, equalTo(APIC_HEADER_LINK))
                .withHeader(NGSILD_TENANT_HEADER, equalTo("urn:ngsi-ld:tenant:01"))
                .withoutHeader(AUTHORIZATION)
        )
    }
}
