package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.hamcrest.core.Is
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.reactive.server.WebTestClient

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
        every { neo4jRepository.createEntity(any()) } returns 2
        every { neo4jRepository.checkExistingUrn(any()) } returns true
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC"))
    }

    @Test
    fun `should return a 409 if the entity is already existing`() {
        val jsonLdFile = ClassPathResource("/data/beehive.jsonld")
        every { neo4jRepository.createEntity(any()) } throws AlreadyExistingEntityException("already existing entity urn:ngsi-ld:BeeHive:TESTC")
        // every { neo4jRepository.checkExistingUrn(any())} returns false
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(409)
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
    fun `query by type should return list of entities of type diat__BeeHive`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label.jsonld")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)

        val map1 = mapOf("@id" to "urn:ngsi-ld:BeeHive:HiveRomania", "@type" to arrayOf("https://diatomic.eglobalmark.com/ontology#BeeHive"), "http://xmlns.com/foaf/0.1/name" to arrayOf(mapOf("@value" to "Bucarest")))
        val map2 = mapOf("@id" to "urn:ngsi-ld:BeeHive:TESTC", "@type" to arrayOf("https://diatomic.eglobalmark.com/ontology#BeeHive"), "http://xmlns.com/foaf/0.1/name" to arrayOf(mapOf("@value" to "ParisBeehive12")), "https://uri.etsi.org/ngsi-ld/v1/ontology#connectsTo" to arrayOf(mapOf("@id" to "urn:ngsi-ld:Beekeeper:TEST1")))

        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(map1, map2)
        webClient.get()
                .uri("/ngsi-ld/v1/entities?type=diat__BeeHive")
                .accept(MediaType.valueOf("application/ld+json"))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }
    @Test
    fun `query by type and query (where criteria is property) should return one entity of type diat__BeeHive with specified criteria`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label_and_query.jsonld")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)

        val map = mapOf("@id" to "urn:ngsi-ld:BeeHive:TESTC", "@type" to arrayOf("https://diatomic.eglobalmark.com/ontology#BeeHive"), "http://xmlns.com/foaf/0.1/name" to arrayOf(mapOf("@value" to "ParisBeehive12")), "https://uri.etsi.org/ngsi-ld/v1/ontology#connectsTo" to arrayOf(mapOf("@id" to "urn:ngsi-ld:Beekeeper:TEST1")))
        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(map)
        webClient.get()
                .uri("/ngsi-ld/v1/entities?type=diat__BeeHive&q=foaf__name==ParisBeehive12")
                .accept(MediaType.valueOf("application/ld+json"))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }
    @Test
    fun `query by type and query (where criteria is relationship) should return a list of type diat__BeeHive that connectsTo specified entity`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label_rel_uri.jsonld")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)
        val map = mapOf("@id" to "urn:ngsi-ld:Door:0015", "@type" to arrayOf("https://diatomic.eglobalmark.com/ontology#Door"), "https://diatomic.eglobalmark.com/ontology#DoorNumber" to arrayOf(mapOf("@value" to "15")), "https://uri.etsi.org/ngsi-ld/v1/ontology#connectsTo" to arrayOf(mapOf("@id" to "urn:ngsi-ld:SmartDoor:0021")))
        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(map)
        webClient.get()
                .uri("/ngsi-ld/v1/entities?type=diat__Door&q=ngsild__connectsTo==urn:ngsi-ld:SmartDoor:0021")
                .accept(MediaType.valueOf("application/ld+json"))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }
}
