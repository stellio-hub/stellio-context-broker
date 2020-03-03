package com.egm.datahub.context.search.service

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource

import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier

// TODO : it should be possible to have this test not depend on the whole application
//        (ie to only load the target service)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)

@ActiveProfiles("test")
class ContextRegistryServiceTests {

    @Autowired
    private lateinit var contextRegistryService: ContextRegistryService

    private lateinit var wireMockServer: WireMockServer

    @BeforeAll
    fun beforeAll() {
        wireMockServer = WireMockServer(wireMockConfig().port(8089))
        wireMockServer.start()
        // If not using the default port, we need to instruct explicitely the client (quite redundant)
        configureFor(8089)
    }

    @AfterAll
    fun afterAll() {
        wireMockServer.stop()
    }

    @Test
    fun `it should call the context registry with the correct entityId`() {

        // prepare our stub
        stubFor(get(urlMatching("/ngsi-ld/v1/entities/entityId"))
            .willReturn(okJson(loadSampleData("beehive.jsonld"))))

        val entity = contextRegistryService.getEntityById("entityId", "Bearer 1234")

        // verify the steps in getEntityById
        StepVerifier.create(entity)
            .expectNextMatches { it.first["@id"] == "urn:ngsi-ld:BeeHive:TESTC" }
            .expectComplete()
            .verify()

        // ensure external components and services have been called as expected
        verify(getRequestedFor(urlPathEqualTo("/ngsi-ld/v1/entities/entityId"))
            .withHeader("Authorization", equalTo("Bearer 1234")))
    }

    private fun loadSampleData(filename: String): String {
        val sampleData = ClassPathResource("/ngsild/$filename")
        return String(sampleData.inputStream.readAllBytes())
    }
}