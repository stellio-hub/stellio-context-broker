package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.hamcrest.core.Is
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.reactive.server.WebTestClient
import java.io.File
import java.nio.file.Files


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = ["entities"])
class EntityHandlerTests {

    companion object {
        init {
            System.setProperty(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers")
        }
    }

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var neo4jRepository: Neo4jRepository

    @Test
    fun `should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/data/beehive.jsonld")
        every { neo4jRepository.createEntity(any()) } returns 7
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC"))
    }

    @Test
    fun `should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/data/beehive_missing_context.jsonld")
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isBadRequest
    }

    @Test
    fun `should return one entity of type diat__BeeHive`() {
        val jsonLdFile = ClassPathResource("/mock/response_getEntitiesByLabel.jsonld")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)

        val map = mapOf("@id" to "urn:ngsi-ld:BeeHive:TESTC", "@type" to arrayOf("https://diatomic.eglobalmark.com/ontology#BeeHive"), "http://xmlns.com/foaf/0.1/name" to arrayOf(mapOf("@value" to "ParisBeehive12")), "https://uri.etsi.org/ngsi-ld/v1/ontology#connectsTo" to arrayOf(mapOf("@id" to "urn:ngsi-ld:Beekeeper:TEST1")))
        every { neo4jRepository.getEntitiesByLabel(any()) } returns listOf(map)
        webClient.get()
                .uri("/ngsi-ld/v1/entities?type=diat__BeeHive")
                .accept(MediaType.valueOf("application/ld+json"))
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }
    
}
