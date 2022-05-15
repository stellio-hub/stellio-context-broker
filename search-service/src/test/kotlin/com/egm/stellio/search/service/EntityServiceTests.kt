package com.egm.stellio.search.service

import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityService::class])
@ActiveProfiles("test")
class EntityServiceTests {

    @Autowired
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var applicationProperties: ApplicationProperties

    private lateinit var wireMockServer: WireMockServer

    @BeforeAll
    fun beforeAll() {
        wireMockServer = WireMockServer(wireMockConfig().port(8089))
        wireMockServer.start()
        // If not using the default port, we need to instruct explicitly the client (quite redundant)
        configureFor(8089)
    }

    @AfterAll
    fun afterAll() {
        wireMockServer.stop()
    }

    @Test
    fun `it should call the context registry with the correct entityId`() {
        // prepare our stub
        stubFor(
            get(urlMatching("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC"))
                .willReturn(okJson(loadSampleData("beehive.jsonld")))
        )

        every {
            applicationProperties.entity
        } returns ApplicationProperties.Entity("http://localhost:8089/ngsi-ld/v1".toUri(), false)

        val entity = entityService.getEntityById("urn:ngsi-ld:BeeHive:TESTC".toUri(), "Bearer 1234")

        // verify the steps in getEntityById
        StepVerifier.create(entity)
            .expectNextMatches { it.id == "urn:ngsi-ld:BeeHive:TESTC" }
            .expectComplete()
            .verify()

        // ensure external components and services have been called as expected
        verify(
            getRequestedFor(urlPathEqualTo("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC"))
                .withHeader("Authorization", equalTo("Bearer 1234"))
        )
    }
}
