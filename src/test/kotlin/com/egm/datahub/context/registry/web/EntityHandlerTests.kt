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
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.web.reactive.server.WebTestClient
import java.lang.RuntimeException

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
                .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isCreated
                .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC"))

        verify(timeout = 1000, exactly = 1) { repositoryEventsListener.handleRepositoryEvent(match { entityEvent ->
            entityEvent.entityType == "BeeHive" &&
                    entityEvent.entityUrn == "urn:ngsi-ld:BeeHive:TESTC" &&
                    entityEvent.operation == EventType.POST
        }) }
        confirmVerified(repositoryEventsListener)
    }

    @Test
    fun `should return a 409 if the entity is already existing`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive.json")

        every { neo4jRepository.checkExistingUrn(any()) } returns true
        webClient.post()
                .uri("/ngsi-ld/v1/entities")
                .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .syncBody(jsonLdFile)
                .exchange()
                .expectStatus().isEqualTo(409)

        verify { repositoryEventsListener wasNot Called }
    }

    @Test
    fun `should return a 500 error if internal server Error`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive.json")

        every { neo4jRepository.createEntity(any(), any(), any()) } throws InternalErrorException("Internal Server Exception")
        every { neo4jRepository.checkExistingUrn(any()) } returns false
        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .syncBody(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(500)

        verify { repositoryEventsListener wasNot Called }
    }

    @Test
    fun `should return a 400 and bad request in Json if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .syncBody(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `query by id should return 200 when the entity is found`() {

        every { neo4jRepository.checkExistingUrn(any()) } returns true

        val jsonLdFile = ClassPathResource("/ngsild/beehive.json")
        val contentJson = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)
        // creating a fake node
        val map1 = "{\n" + "}"
        val entityMap1: Map<String, Any> = gson.fromJson(map1, object : TypeToken<Map<String, Any>>() {}.type)
        var node1: NodeModel = NodeModel(27647624)
        node1.setProperties(entityMap1)
        every { neo4jRepository.getNodeByURI(any()) } returns mapOf("n" to node1)

        val resp1: Map<String, Any> = gson.fromJson(contentJson, object : TypeToken<Map<String, Any>>() {}.type)
        every { neo4jService.queryResultToNgsiLd(node1) } returns resp1
        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TESTC")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(contentJson)
    }

    @Test
    fun `query by id should return 404 and json not found`() {

        every { neo4jRepository.checkExistingUrn(any()) } returns false

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isEqualTo(404)
    }

    @Test
    fun `query by type should return 400 if bad request`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities?type=diat__Bee")
            .accept(MediaType.valueOf("application/ld+json"))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `should return a 400 if Link header NS does not match the entity type`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")
        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .syncBody(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `query by type should return list of entities of type BeeHive`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label.json")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)
        val map1 = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"name\": \"Bucarest\",\n" +
                "  \"uri\": \"urn:ngsi-ld:BeeHive:HiveRomania\"\n" +
                "}"
        val map2 = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"name\": \"ParisBeehive12\",\n" +
                "  \"uri\": \"urn:ngsi-ld:BeeHive:TESTC\"\n" +
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
                "    \"id\": \"urn:ngsi-ld:BeeHive:TESTC\",\n" +
                "    \"type\": \"BeeHive\",\n" +
                "    \"connectsTo\": {\n" +
                "      \"type\": \"Relationship\",\n" +
                "      \"object\": \"urn:ngsi-ld:Beekeeper:Pascal\",\n" +
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
                "    \"id\": \"urn:ngsi-ld:BeeHive:HiveRomania\",\n" +
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
                .uri("/ngsi-ld/v1/entities?type=BeeHive")
                .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }

    @Test
    fun `query by type and query (where criteria is property) should return one entity of type BeeHive with specified criteria`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label_and_query.json")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)

        val map = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"name\": \"ParisBeehive12\",\n" +
                "  \"uri\": \"urn:ngsi-ld:BeeHive:TESTC\"\n" +
                "}"
        val entityMap: Map<String, Any> = gson.fromJson(map, object : TypeToken<Map<String, Any>>() {}.type)
        var node: NodeModel = NodeModel(27647624)
        node.setProperties(entityMap)
        node.labels = arrayOf("diat__BeeHive")
        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(mapOf("n" to node))

        val ngsild = "{\n" +
                "    \"id\": \"urn:ngsi-ld:BeeHive:TESTC\",\n" +
                "    \"type\": \"BeeHive\",\n" +
                "    \"connectsTo\": {\n" +
                "      \"type\": \"Relationship\",\n" +
                "      \"object\": \"urn:ngsi-ld:Beekeeper:Pascal\",\n" +
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
                .uri("/ngsi-ld/v1/entities?type=BeeHive&q=name==ParisBeehive12")
                .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }
    @Test
    fun `query by type and query (where criteria is relationship) should return a list of type BeeHive that connectsTo specified entity`() {
        val jsonLdFile = ClassPathResource("/mock/response_entities_by_label_rel_uri.json")
        val content = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8)
        val map = "{\n" +
                "  \"createdAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"doorNumber\": \"15\",\n" +
                "  \"modifiedAt\": \"2019.10.09.11.53.05\",\n" +
                "  \"uri\": \"urn:ngsi-ld:Door:0015\"\n" +
                "}"
        val entityMap: Map<String, Any> = gson.fromJson(map, object : TypeToken<Map<String, Any>>() {}.type)
        var node: NodeModel = NodeModel(576576)
        node.setProperties(entityMap)
        node.labels = arrayOf("diat__Door")
        every { neo4jRepository.getEntities(any(), any()) } returns arrayListOf(mapOf("n" to node))

        val ngsild = "{\n" +
                "    \"id\": \"urn:ngsi-ld:Door:0015\",\n" +
                "    \"type\": \"Door\",\n" +
                "    \"connectsTo\": {\n" +
                "      \"type\": \"Relationship\",\n" +
                "      \"object\": \"urn:ngsi-ld:SmartDoor:0021\"\n" +
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
                .uri("/ngsi-ld/v1/entities?type=Door&q=connectsTo==urn:ngsi-ld:SmartDoor:0021")
                .accept(MediaType.valueOf("application/ld+json"))
                .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .exchange()
                .expectStatus().isOk
                .expectBody().json(content)
    }

    @Test
    fun `entity attribute update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update_attribute.json")

        every { neo4jRepository.updateEntity(any()) } returns emptyMap()
        val entityId = "urn:ngsi-ld:Sensor:0022CCC"
        val attrId = "name"

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", "<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .syncBody(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { neo4jRepository.updateEntity(any()) }
        confirmVerified(neo4jRepository)
    }

    @Test
    fun `entity update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update.json")

        every { neo4jRepository.updateEntity(any()) } returns emptyMap()
        val entityId = "urn:ngsi-ld:Sensor:0022CCC"

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<http://easyglobalmarket.com/contexts/sosa.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .syncBody(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { neo4jRepository.updateEntity(any()) }
        confirmVerified(neo4jRepository)
    }

    @Test
    fun `entity update should return a 400 if JSON-LD context is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update.json")

        val entityId = "urn:ngsi-ld:Sensor:0022CCC"

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .syncBody(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest

        verify { neo4jRepository wasNot Called }
    }

    @Test
    fun `entity update should return a 400 if entity type is unknown`() {
        val jsonLdFile = ClassPathResource("/ngsild/sensor_update.json")

        val entityId = "urn:ngsi-ld:UnknownType:0022CCC"

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .syncBody(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest

        verify { neo4jRepository wasNot Called }
    }

    @Test
    fun `it should return a 204 if an entity has been successfully deleted`() {
        every { neo4jRepository.deleteEntity(any()) } returns Pair(1, 1)

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .accept(MediaType.valueOf("application/ld+json"))
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { neo4jRepository.deleteEntity(eq("urn:ngsi-ld:Sensor:0022CCC")) }
        confirmVerified(neo4jRepository)
    }

    @Test
    fun `it should return a 404 if entity to be deleted has not been found`() {
        every { neo4jRepository.deleteEntity(any()) } returns Pair(0, 0)

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .accept(MediaType.valueOf("application/ld+json"))
            .exchange()
            .expectStatus().isNotFound
            .expectBody().isEmpty
    }

    @Test
    fun `it should return a 500 if entity could not be deleted`() {
        every { neo4jRepository.deleteEntity(any()) } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .accept(MediaType.valueOf("application/ld+json"))
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json("{\"ProblemDetails\":[\"Unexpected server error\"]}")
    }
}
