package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.NgsiLdEntity
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@AutoConfigureWebTestClient(timeout = "30000")
@ActiveProfiles("test")
@WebFluxTest(EntityOperationHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", username = "mock-user")
class EntityOperationHandlerTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityOperationService: EntityOperationService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @Test
    fun `create batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
            "urn:ngsi-ld:Device:HCMR-AQUABOX1"
        )
        val expandedEntities = slot<List<NgsiLdEntity>>()

        every { entityOperationService.splitEntitiesByExistence(capture(expandedEntities)) } returns Pair(
            emptyList(),
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            entitiesIds,
            arrayListOf()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { authorizationService.createAdminLink(any(), eq("mock-user")) } just runs

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

        verify { authorizationService.createAdminLinks(entitiesIds, "mock-user") }
        confirmVerified()
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
            emptyList(),
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds,
            errors
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true

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

        verify { authorizationService.createAdminLinks(createdEntitiesIds, "mock-user") }
        confirmVerified()
    }

    @Test
    fun `create batch entity should return a 200 when some entities already exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
            "urn:ngsi-ld:Device:HCMR-AQUABOX1"
        )
        val existingEntity = mockk<NgsiLdEntity>()
        every { existingEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen"

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(existingEntity),
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds,
            arrayListOf()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true

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

        verify { authorizationService.createAdminLinks(createdEntitiesIds, "mock-user") }
        confirmVerified()
    }

    @Test
    fun `create batch should not authorize user without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { authorizationService.userCanCreateEntities("mock-user") } returns false

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden to create entities"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `upsert batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature"
        )
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
            "urn:ngsi-ld:Device:HCMR-AQUABOX1"
        )
        val createdBatchResult = BatchOperationResult(
            createdEntitiesIds,
            arrayListOf()
        )

        val existingEntities = emptyList<NgsiLdEntity>()
        val nonExistingEntities = emptyList<NgsiLdEntity>()

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            nonExistingEntities
        )

        every { entityOperationService.create(nonExistingEntities) } returns createdBatchResult
        every { entityOperationService.update(existingEntities, createdBatchResult) } returns BatchOperationResult(
            entitiesIds,
            arrayListOf()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user") } returns emptyList()

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert?options=update")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "errors": [],
                    success: [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                        "urn:ngsi-ld:Device:HCMR-AQUABOX1"
                    ]
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(createdEntitiesIds, "mock-user") }
        confirmVerified()
    }

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
            emptyList(),
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every { entityOperationService.update(any(), any()) } returns BatchOperationResult(
            arrayListOf(),
            errors
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user") } returns emptyList()

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert?options=update")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                { 
                    "errors": [
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", 
                            "error": [ 
                                "Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist." 
                            ] 
                        }, 
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", 
                            "error": [ 
                                "Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist." 
                            ] 
                        }
                    ], 
                    "success": [] 
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(emptyList(), "mock-user") }
        confirmVerified()
    }

    @Test
    fun `upsert batch entity without option should replace entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
            "urn:ngsi-ld:Device:HCMR-AQUABOX1"
        )
        val existingEntities = emptyList<NgsiLdEntity>()

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every { entityOperationService.replace(existingEntities, any()) } returns BatchOperationResult(
            entitiesIds,
            arrayListOf()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user") } returns emptyList()

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "errors": [],
                    success: [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                        "urn:ngsi-ld:Device:HCMR-AQUABOX1"
                    ]
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(emptyList(), "mock-user") }
        confirmVerified()
    }

    @Test
    fun `upsert batch should not authorize user to create entities without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { authorizationService.userCanCreateEntities("mock-user") } returns false

        val expandedEntity = mockk<NgsiLdEntity>()
        every { expandedEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature"
        val nonExistingEntities = listOf(expandedEntity)

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            nonExistingEntities
        )
        every { entityOperationService.replace(emptyList(), any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns false
        every { authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user") } returns emptyList()

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "errors": [
                        {
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                            "error": [
                                "User forbidden to create entities"
                            ]
                        }
                    ],
                    "success": []
                }
                """.trimIndent()
            )
    }

    @Test
    fun `upsert batch should not authorize user to updates entities without write right`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { authorizationService.userCanCreateEntities("mock-user") } returns false

        val entitiesId =
            listOf("urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen")

        val expandedEntity = mockk<NgsiLdEntity>()
        every { expandedEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature"
        val expandedEntity2 = mockk<NgsiLdEntity>()
        every { expandedEntity2.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen"
        val existingEntities = listOf(expandedEntity, expandedEntity2)

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            emptyList()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns false

        val entitiesIdToUpdate = listOf(expandedEntity.id)
        every {
            authorizationService.filterEntitiesUserCanUpdate(
                entitiesId,
                "mock-user"
            )
        } returns entitiesIdToUpdate

        every { entityOperationService.replace(listOf(expandedEntity), any()) } returns BatchOperationResult(
            ArrayList(entitiesIdToUpdate),
            arrayListOf()
        )

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert")
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
                                "User forbidden to modify entity"
                            ]
                        }
                    ],
                    "success": [ "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature" ]
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(emptyList(), "mock-user") }
        confirmVerified()
    }

    @Test
    fun `create batch entity should return a 400 if JSON-LD payload is not correct`() {
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        shouldReturn400WithBadPayload("create")
    }

    @Test
    fun `upsert batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("upsert")
    }

    private fun shouldReturn400WithBadPayload(method: String) {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_missing_context.json")
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/$method")
            .header(
                "Link",
                "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"
            )
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {"type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                "title":"The request includes input data which does not meet the requirements of the operation",
                "detail":"Unexpected error while parsing payload : Unable to parse input payload"}
                """.trimIndent()
            )
    }

    @Test
    fun `delete batch for correct entities should return a 200 with explicit success message`() {
        val entitiesIds = slot<List<String>>()
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(
                entitiesIds.captured,
                emptyList()
            )
        }
        val canAdminEntitiesIds = slot<List<String>>()
        every { authorizationService.splitEntitiesByUserCanAdmin(capture(canAdminEntitiesIds), any()) } answers {
            Pair(
                canAdminEntitiesIds.captured,
                emptyList()
            )
        }

        val computedEntitiesIdsToDelete = slot<Set<String>>()
        every { entityOperationService.delete(capture(computedEntitiesIdsToDelete)) } returns
            BatchOperationResult(
                mutableListOf(
                    "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                    "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                    "urn:ngsi-ld:Device:HCMR-AQUABOX1"
                ),
                mutableListOf()
            )

        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/delete")
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
    }

    @Test
    fun `delete batch for unknown entities should return a 200 with explicit error messages`() {
        val entitiesIds = slot<List<String>>()
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(
                emptyList(),
                entitiesIds.captured
            )
        }
        val existingEntitiesIds = slot<List<String>>()
        every { authorizationService.splitEntitiesByUserCanAdmin(capture(existingEntitiesIds), any()) } answers {
            Pair(
                emptyList(),
                emptyList()
            )
        }

        val computedEntitiesIdsToDelete = slot<Set<String>>()
        every { entityOperationService.delete(capture(computedEntitiesIdsToDelete)) } answers {
            BatchOperationResult(
                computedEntitiesIdsToDelete.captured.toMutableList(),
                mutableListOf()
            )
        }

        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/delete")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
            {
                "success": [],
                "errors": [
                    {"entityId":"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature","error":["Entity does not exist"]},
                    {"entityId":"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen","error":["Entity does not exist"]},
                    {"entityId":"urn:ngsi-ld:Device:HCMR-AQUABOX1","error":["Entity does not exist"]}
                ]
            }
                """.trimIndent()
            )
        assertEquals(emptyList<String>(), existingEntitiesIds.captured)
        assertEquals(emptySet<String>(), computedEntitiesIdsToDelete.captured)
    }

    @Test
    fun `delete batch for unauthorized entities should return a 200 with explicit error messages`() {
        val entitiesIds = slot<List<String>>()
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(
                entitiesIds.captured,
                emptyList()
            )
        }
        val existingEntitiesIds = slot<List<String>>()
        every { authorizationService.splitEntitiesByUserCanAdmin(capture(existingEntitiesIds), any()) } answers {
            Pair(
                emptyList(),
                existingEntitiesIds.captured
            )
        }

        val computedEntitiesIdsToDelete = slot<Set<String>>()
        every { entityOperationService.delete(capture(computedEntitiesIdsToDelete)) } answers {
            BatchOperationResult(
                computedEntitiesIdsToDelete.captured.toMutableList(),
                mutableListOf()
            )
        }

        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/delete")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
    {
        "success": [],
        "errors": [
            {"entityId":"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature","error":["User forbidden to delete entity"]},
            {"entityId":"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen","error":["User forbidden to delete entity"]},
            {"entityId":"urn:ngsi-ld:Device:HCMR-AQUABOX1","error":["User forbidden to delete entity"]}
        ]
    }
                """.trimIndent()
            )
        assertEquals(emptySet<String>(), computedEntitiesIdsToDelete.captured)
    }
}
