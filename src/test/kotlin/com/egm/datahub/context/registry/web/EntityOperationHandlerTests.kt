package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.service.Neo4jService
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@AutoConfigureWebTestClient(timeout = "30000")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class EntityOperationHandlerTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var neo4jService: Neo4jService

    @Test
    fun `create batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf("urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", "urn:ngsi-ld:Device:HCMR-AQUABOX1")

        every { neo4jService.exists(any()) } returns false
        every { neo4jService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = arrayListOf())

        webClient.post()
                .uri("/ngsi-ld/v1/entities/entityOperations/create")
                .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
                .accept(MediaType.valueOf("application/ld+json"))
                .bodyValue(jsonLdFile)
                .exchange()
                .expectStatus().isOk
                .expectBody().json("{\n" +
                    "    \"errors\": [],\n" +
                    "    \"success\": [\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature\",\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen\",\n" +
                    "        \"urn:ngsi-ld:Device:HCMR-AQUABOX1\"\n" +
                    "    ]\n" +
                    "}")
    }

    @Test
    fun `create batch entity should return a 200 if if JSON-LD payload is correct and has cyclic dependency`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_cyclic_dependency.json")
        val createdEntitiesIds = arrayListOf("urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", "urn:ngsi-ld:Device:HCMR-AQUABOX1")

        every { neo4jService.exists(any()) } returns false
        every { neo4jService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = arrayListOf())

        webClient.post()
            .uri("/ngsi-ld/v1/entities/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\n" +
                    "    \"errors\": [],\n" +
                    "    \"success\": [\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature\",\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen\",\n" +
                    "        \"urn:ngsi-ld:Device:HCMR-AQUABOX1\"\n" +
                    "    ]\n" +
                    "}")
    }

    @Test
    fun `create batch entity should return a 200 when some entities are not valid`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_not_valid.json")
        val createdEntitiesIds = arrayListOf("urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", "urn:ngsi-ld:Device:HCMR-AQUABOX1")
        val errorObject = arrayListOf(BatchEntityError("urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 in relationship connectsTo does not exist, create it first")))

        every { neo4jService.exists(any()) } returns false
        every { neo4jService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = errorObject)

        webClient.post()
            .uri("/ngsi-ld/v1/entities/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\n" +
                    "    \"errors\": [\n" +
                    "        {\n" +
                    "            \"entityId\": \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen\",\n" +
                    "            \"error\": [\n" +
                    "                \"Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 in relationship connectsTo does not exist, create it first\"\n" +
                    "            ]\n" +
                    "        }\n" +
                    "    ],\n" +
                    "    \"success\": [\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature\",\n" +
                    "        \"urn:ngsi-ld:Device:HCMR-AQUABOX1\"\n" +
                    "    ]\n" +
                    "}")
    }

    @Test
    fun `create batch entity should return a 200 when some entities already exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf("urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", "urn:ngsi-ld:Device:HCMR-AQUABOX1")
        val errorObject = arrayListOf(BatchEntityError("urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", arrayListOf("Entity already exists")))

        every { neo4jService.exists(any()) } returns false
        every { neo4jService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = errorObject)

        webClient.post()
            .uri("/ngsi-ld/v1/entities/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("{\n" +
                    "    \"errors\": [\n" +
                    "        {\n" +
                    "            \"entityId\": \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen\",\n" +
                    "            \"error\": [\n" +
                    "                \"Entity already exists\"\n" +
                    "            ]\n" +
                    "        }\n" +
                    "    ],\n" +
                    "    \"success\": [\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature\",\n" +
                    "        \"urn:ngsi-ld:Device:HCMR-AQUABOX1\"\n" +
                    "    ]\n" +
                    "}")
    }

    @Test
    fun `create batch entity should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_missing_context.json")

        webClient.post()
            .uri("/ngsi-ld/v1/entities/entityOperations/create")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .accept(MediaType.valueOf("application/ld+json"))
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }
}
