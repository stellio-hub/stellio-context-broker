package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.Entity
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier

// TODO : it should be possible to have this test not depend on the whole application
//        (ie to only load the target service)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = ["entities"])
@ActiveProfiles("test")
class ContextRegistryServiceTests {

    @Autowired
    private lateinit var contextRegistryService: ContextRegistryService

    @MockkBean
    private lateinit var ngsiLdParsingService: NgsiLdParsingService

    private lateinit var wireMockServer: WireMockServer

    companion object {
        init {
            System.setProperty(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers")
        }
    }

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
        val beehive = """
            {
              "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                "https://diatomic.eglobalmark.com/diatomic-context.jsonld"
              ],
              "id": "urn:diat:BeeHive:TESTC",
              "type": "BeeHive",
              "name": "ParisBeehive12",
              "connectsTo": {
                "type": "Relationship",
                "createdAt": "2010-10-26T21:32:52+02:00",
                "object": "urn:diat:Beekeeper:Pascal"
              }
            }
        """

        // prepare our stub and mock
        stubFor(get(urlMatching("/ngsi-ld/v1/entities/entityId"))
            .willReturn(okJson(beehive)))
        every { ngsiLdParsingService.parse(any()) } returns Entity().also { it.addProperty("id", "urn:diat:BeeHive:TESTC") }

        val entity = contextRegistryService.getEntityById("entityId")

        // verify the steps in getEntityById
        StepVerifier.create(entity)
            .expectNextMatches { it.getPropertyValue("id") == "urn:diat:BeeHive:TESTC" }
            .expectComplete()
            .verify()

        // ensure external components and services have been called as expected
        verify(getRequestedFor(urlPathEqualTo("/ngsi-ld/v1/entities/entityId")))
        io.mockk.verify { ngsiLdParsingService.parse(eq(beehive)) }
        confirmVerified(ngsiLdParsingService)
    }
}