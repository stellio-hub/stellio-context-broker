package com.egm.stellio.entity.web

import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.entity.util.ValidationUtils
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@AutoConfigureWebTestClient(timeout = "30000")
@ActiveProfiles("test")
@WebFluxTest(EntityOperationHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockUser
class EntityOperationHandlerTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var validationUtils: ValidationUtils

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @Test
    fun `create batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf("urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", "urn:ngsi-ld:Device:HCMR-AQUABOX1")

        every { entityService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = arrayListOf())

        webClient.post()
                .uri("/ngsi-ld/v1/entityOperations/create")
                .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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

        every { entityService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = arrayListOf())

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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

        every { entityService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = errorObject)

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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

        every { entityService.processBatchOfEntities(any(), any(), any()) } returns BatchOperationResult(success = createdEntitiesIds, errors = errorObject)

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"Could not parse entity due to invalid json-ld payload\"}")
    }
}
