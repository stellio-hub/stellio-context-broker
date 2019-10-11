package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.model.EventType
import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.egm.datahub.context.registry.service.Neo4jService
import com.egm.datahub.context.registry.service.RepositoryEventsListener
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.hamcrest.core.Is
import org.junit.jupiter.api.Test
import org.neo4j.ogm.response.model.NodeModel
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
        private val gson = GsonBuilder().setPrettyPrinting().create()
    }

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var neo4jRepository: Neo4jRepository

    @MockkBean
    private lateinit var repositoryEventsListener: RepositoryEventsListener

    @MockkBean
    private lateinit var neo4jService: Neo4jService

    @Test
    fun `should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive.json")
        every { neo4jRepository.createEntity(any(), any(), any()) } returns ""
        every { neo4jRepository.checkExistingUrn(any()) } returns false
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/urn:diat:BeeHive:TESTC"))

        verify(timeout = 1000, exactly = 1) { repositoryEventsListener.handleRepositoryEvent(match { entityEvent ->
            entityEvent.entityType == "BeeHive" &&
                    entityEvent.entityUrn == "urn:diat:BeeHive:TESTC" &&
                    entityEvent.operation == EventType.POST
        }) }
        confirmVerified(repositoryEventsListener)
    }

    @Test
    fun `should return a 409 if the entity is already existing`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive.json")
        every { neo4jRepository.createEntity(any(), any(), any()) } throws AlreadyExistingEntityException("already existing entity urn:ngsi-ld:BeeHive:TESTC")
        every { neo4jRepository.checkExistingUrn(any()) } returns true
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(409)

        verify { repositoryEventsListener wasNot Called }
    }

    @Test
    fun `should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isBadRequest
    }

    @Test
    fun `query by type should return list of entities of type diat__BeeHive`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label.json")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)

        val map1 = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"name\": \"Bucarest\",\n" +
                "  \"uri\": \"urn:diat:BeeHive:HiveRomania\"\n" +
                "}"
        val map2 = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"name\": \"ParisBeehive12\",\n" +
                "  \"uri\": \"urn:diat:BeeHive:TESTC\"\n" +
                "}"
        val entityMap1: Map<String, Any> = gson.fromJson(map1, object : TypeToken<Map<String, Any>>() {}.type)
        var node1: NodeModel = NodeModel(27647624)
        node1.setProperties(entityMap1)
        node1.labels = arrayOf("diat__BeeHive")
        val entityMap2: Map<String, Any> = gson.fromJson(map1, object : TypeToken<Map<String, Any>>() {}.type)
        var node2: NodeModel = NodeModel(27565624)
        node2.setProperties(entityMap2)
        node2.labels = arrayOf("diat__BeeHive")

        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(mapOf("n" to node1), mapOf("n" to node2))

        val ngsild2 = "{\n" +
                "    \"id\": \"urn:diat:BeeHive:TESTC\",\n" +
                "    \"type\": \"BeeHive\",\n" +
                "    \"connectsTo\": {\n" +
                "      \"type\": \"Relationship\",\n" +
                "      \"object\": \"urn:diat:Beekeeper:Pascal\",\n" +
                "      \"createdAt\": \"2010-10-26T21:32:52+02:00\",\n" +
                "      \"modifiedAt\": \"2019.10.09.11.53.05\"\n" +
                "    },\n" +
                "    \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"name\": \"ParisBeehive12\",\n" +
                "    \"@context\": [\n" +
                "      \"https://diatomic.eglobalmark.com/diatomic-context.jsonld\",\n" +
                "      \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\",\n" +
                "      \"https://fiware.github.io/dataModels/fiware-datamodels-context.jsonld\"\n" +
                "    ]\n" +
                "  }"
        val ngsild1 = "{\n" +
                "    \"id\": \"urn:diat:BeeHive:HiveRomania\",\n" +
                "    \"type\": \"BeeHive\",\n" +
                "    \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"name\": \"Bucarest\",\n" +
                "    \"@context\": [\n" +
                "      \"https://diatomic.eglobalmark.com/diatomic-context.jsonld\",\n" +
                "      \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\",\n" +
                "      \"https://fiware.github.io/dataModels/fiware-datamodels-context.jsonld\"\n" +
                "    ]\n" +
                "  }"
        val resp1: Map<String, Any> = gson.fromJson(ngsild1, object : TypeToken<Map<String, Any>>() {}.type)
        val resp2: Map<String, Any> = gson.fromJson(ngsild2, object : TypeToken<Map<String, Any>>() {}.type)

        every { neo4jService.queryResultToNgsiLd(node1) } returns resp1
        every { neo4jService.queryResultToNgsiLd(node2) } returns resp2

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
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label_and_query.json")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)

        val map = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"name\": \"ParisBeehive12\",\n" +
                "  \"uri\": \"urn:diat:BeeHive:TESTC\"\n" +
                "}"
        val entityMap: Map<String, Any> = gson.fromJson(map, object : TypeToken<Map<String, Any>>() {}.type)
        var node: NodeModel = NodeModel(27647624)
        node.setProperties(entityMap)
        node.labels = arrayOf("diat__BeeHive")
        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(mapOf("n" to node))

        val ngsild = "{\n" +
                "    \"id\": \"urn:diat:BeeHive:TESTC\",\n" +
                "    \"type\": \"BeeHive\",\n" +
                "    \"connectsTo\": {\n" +
                "      \"type\": \"Relationship\",\n" +
                "      \"object\": \"urn:diat:Beekeeper:Pascal\",\n" +
                "      \"createdAt\": \"2010-10-26T21:32:52+02:00\",\n" +
                "      \"modifiedAt\": \"2019.10.09.11.53.05\"\n" +
                "    },\n" +
                "    \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"name\": \"ParisBeehive12\",\n" +
                "    \"@context\": [\n" +
                "      \"https://diatomic.eglobalmark.com/diatomic-context.jsonld\",\n" +
                "      \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\",\n" +
                "      \"https://fiware.github.io/dataModels/fiware-datamodels-context.jsonld\"\n" +
                "    ]\n" +
                "  }"
        val resp: Map<String, Any> = gson.fromJson(ngsild, object : TypeToken<Map<String, Any>>() {}.type)

        every { neo4jService.queryResultToNgsiLd(node) } returns resp
        webClient.get()
                .uri("/ngsi-ld/v1/entities?type=diat__BeeHive&q=name==ParisBeehive12")
                .accept(MediaType.valueOf("application/ld+json"))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }
    @Test
    fun `query by type and query (where criteria is relationship) should return a list of type diat__BeeHive that connectsTo specified entity`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label_rel_uri.json")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)
        val map = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"doorNumber\": \"15\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"uri\": \"urn:diat:Door:0015\"\n" +
                "}"
        val entityMap: Map<String, Any> = gson.fromJson(map, object : TypeToken<Map<String, Any>>() {}.type)
        var node: NodeModel = NodeModel(576576)
        node.setProperties(entityMap)
        node.labels = arrayOf("diat__Door")
        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(mapOf("n" to node))

        val ngsild = "{\n" +
                "    \"id\": \"urn:diat:Door:0015\",\n" +
                "    \"type\": \"Door\",\n" +
                "    \"connectsTo\": {\n" +
                "      \"type\": \"Relationship\",\n" +
                "      \"object\": \"urn:diat:SmartDoor:0021\"\n" +
                "    },\n" +
                "    \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"doorNumber\": \"15\",\n" +
                "    \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "    \"@context\": [\n" +
                "      \"https://diatomic.eglobalmark.com/diatomic-context.jsonld\",\n" +
                "      \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\",\n" +
                "      \"https://fiware.github.io/dataModels/fiware-datamodels-context.jsonld\"\n" +
                "    ]\n" +
                "  }"
        val resp: Map<String, Any> = gson.fromJson(ngsild, object : TypeToken<Map<String, Any>>() {}.type)
        every { neo4jService.queryResultToNgsiLd(node) } returns resp

        webClient.get()
                .uri("/ngsi-ld/v1/entities?type=diat__Door&q=ngsild__connectsTo==urn:ngsild:SmartDoor:0021")
                .accept(MediaType.valueOf("application/ld+json"))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }
}
