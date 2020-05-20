package com.egm.stellio.entity.web

import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.model.ExpandedEntity
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertEquals
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
    private lateinit var entityOperationService: EntityOperationService

    @Test
    fun `upsert batch entity should return a 200 if JSON-LD payload contains update errors`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")
        val errors = arrayListOf(
            BatchEntityError(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.")
            ),
            BatchEntityError(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.")
            )
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(),
            listOf()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every { entityOperationService.update(any()) } returns BatchOperationResult(
            arrayListOf(),
            errors
        )

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                "{\n" +
                    "    \"errors\": [" +
                    "        {\n" +
                    "            \"entityId\": \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature\",\n" +
                    "            \"error\": [\n" +
                    "                \"Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.\"\n" +
                        "            ]\n" +
                        "        },\n" +
                        "        {\n" +
                        "            \"entityId\": \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen\",\n" +
                        "            \"error\": [\n" +
                        "                \"Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.\"\n" +
                        "            ]\n" +
                        "        }],\n" +
                        "    \"success\": []\n" +
                        "}"
            )
    }

    @Test
    fun `upsert batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
            "urn:ngsi-ld:Device:HCMR-AQUABOX1"
        )

        val existingEntities = mockk<List<ExpandedEntity>>()
        val nonExistingEntities = mockk<List<ExpandedEntity>>()

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            nonExistingEntities
        )
        every { entityOperationService.create(nonExistingEntities) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every { entityOperationService.update(existingEntities) } returns BatchOperationResult(
            entitiesIds,
            arrayListOf()
        )
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                "{\n" +
                    "    \"errors\": [],\n" +
                    "    \"success\": [\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature\",\n" +
                    "        \"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen\",\n" +
                    "        \"urn:ngsi-ld:Device:HCMR-AQUABOX1\"\n" +
                    "    ]\n" +
                        "}"
            )
    }

    @Test
    fun `create batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
            "urn:ngsi-ld:Device:HCMR-AQUABOX1"
        )
        val expandedEntities = slot<List<ExpandedEntity>>()

        every { entityOperationService.splitEntitiesByExistence(capture(expandedEntities)) } returns Pair(
            listOf(),
            listOf()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            entitiesIds,
            arrayListOf()
        )

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "errors": [],
                    "success": [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                        "urn:ngsi-ld:Device:HCMR-AQUABOX1"
                    ]
                }
                """.trimIndent()
            )

        assertEquals(entitiesIds, expandedEntities.captured.map { it.id })
    }

    @Test
    fun `create batch entity should return a 200 when an entity is targetting an unknown object`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_not_valid.json")
        val createdEntitiesIds = arrayListOf(
            "urn:ngsi-ld:FishContainment:0012"
        )
        val errors = arrayListOf(
            BatchEntityError(
                "urn:ngsi-ld:MortalityService:014YFA9Z",
                arrayListOf("Target entity urn:ngsi-ld:BreedingService:0214 does not exist")
            ),
            BatchEntityError(
                "urn:ngsi-ld:DeadFishes:019BN",
                arrayListOf("Target entity urn:ngsi-ld:BreedingService:0214 does not exist")
            )
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(),
            listOf()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds,
            errors
        )

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "errors": [
                          {
                            "entityId": "urn:ngsi-ld:MortalityService:014YFA9Z",
                            "error": [
                              "Target entity urn:ngsi-ld:BreedingService:0214 does not exist"
                            ]
                          },
                          {
                            "entityId": "urn:ngsi-ld:DeadFishes:019BN",
                            "error": [
                              "Target entity urn:ngsi-ld:BreedingService:0214 does not exist"
                            ]
                          }
                        ],
                    "success": [
                        "urn:ngsi-ld:FishContainment:0012"
                    ]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create batch entity should return a 200 when some entities already exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
            "urn:ngsi-ld:Device:HCMR-AQUABOX1"
        )
        val existingEntity = mockk<ExpandedEntity>()
        every { existingEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen"

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(existingEntity),
            listOf()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds,
            arrayListOf()
        )

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "errors": [
                        {
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                            "error": [
                                "Entity already exists"
                            ]
                        }
                    ],
                    "success": [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                        "urn:ngsi-ld:Device:HCMR-AQUABOX1"
                    ]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create batch entity should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_missing_context.json")

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header(
                "Link",
                "<http://easyglobalmarket.com/contexts/diat.jsonld>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
            )
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {"type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                "title":"The request includes input data which does not meet the requirements of the operation",
                "detail":"Could not parse entity due to invalid json-ld payload"}
                """.trimIndent()
            )
    }
}