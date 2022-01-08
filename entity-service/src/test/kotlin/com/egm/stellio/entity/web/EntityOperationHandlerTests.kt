package com.egm.stellio.entity.web

import arrow.core.Some
import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.net.URI
import java.util.UUID

@AutoConfigureWebTestClient(timeout = "30000")
@ActiveProfiles("test")
@WebFluxTest(EntityOperationHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", sub = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")
class EntityOperationHandlerTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityOperationService: EntityOperationService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    private val sub = Some(UUID.fromString("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"))
    private val batchSomeEntitiesExistsResponse =
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

    private val batchUpsertOrUpdateWithUpdateErrorsResponse =
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

    private val batchUpsertWithoutWriteRightResponse =
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

    private val deviceAquaBox1 = "urn:ngsi-ld:Device:HCMR-AQUABOX1"

    private val sensorType = "https://ontology.eglobalmark.com/egm#Sensor"
    private val deviceType = "https://ontology.eglobalmark.com/egm#Device"
    private val deviceParameterAttribute = "https://ontology.eglobalmark.com/aquac#deviceParameter"

    private val batchCreateEndpoint = "/ngsi-ld/v1/entityOperations/create"
    private val batchUpsertEndpoint = "/ngsi-ld/v1/entityOperations/upsert"
    private val batchUpsertWithUpdateEndpoint = "/ngsi-ld/v1/entityOperations/upsert?options=update"
    private val batchUpdateEndpoint = "/ngsi-ld/v1/entityOperations/update"
    private val batchDeleteEndpoint = "/ngsi-ld/v1/entityOperations/delete"

    private val hcmrContext = listOf(
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/shared-jsonld-contexts/egm.jsonld",
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/aquac/jsonld-contexts/aquac.jsonld",
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    )

    private val upsertUpdateBatchOperationResult = BatchOperationResult(
        mutableListOf(
            BatchEntitySuccess(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
                UpdateResult(
                    listOf(
                        UpdatedDetails(
                            deviceParameterAttribute,
                            null,
                            UpdateOperationResult.APPENDED
                        )
                    ),
                    emptyList()
                )
            ),
            BatchEntitySuccess(
                deviceAquaBox1.toUri(),
                UpdateResult(
                    listOf(
                        UpdatedDetails(
                            "https://ontology.eglobalmark.com/aquac#brandName",
                            "urn:ngsi-ld:Dataset:brandName:01".toUri(),
                            UpdateOperationResult.REPLACED
                        )
                    ),
                    emptyList()
                )
            )
        ),
        mutableListOf()
    )

    @Test
    fun `update batch entity should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(any(), sub)
        } returns entitiesIds
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            emptyList()
        )
        every {
            entityOperationService.update(any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty
    }

    @Test
    fun `update batch entity should return a 207 if JSON-LD payload contains update errors`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")
        val errors = arrayListOf(
            BatchEntityError(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
                arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.")
            ),
            BatchEntityError(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
                arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.")
            )
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            emptyList()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), sub)
        } returns emptyList()
        every { entityOperationService.update(any(), any()) } returns BatchOperationResult(
            mutableListOf(),
            errors
        )

        webClient.post()
            .uri(batchUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(batchUpsertOrUpdateWithUpdateErrorsResponse)
    }

    @Test
    fun `update batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("update")
    }

    @Test
    fun `update batch entity should return 204 if JSON-LD payload is correct and noOverwrite is asked`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(any(), sub)
        } returns entitiesIds
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            emptyList()
        )
        every {
            entityOperationService.update(any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/update?options=noOverwrite")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { entityOperationService.update(any(), true) }

        confirmVerified()
    }

    @Test
    fun `update batch entity should return 207 if there is a non existing entity in the payload`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val updatedEntitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX200".toUri()
        )
        val nonExistingEntity = mockk<NgsiLdEntity>()

        every { nonExistingEntity.id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX200".toUri()
        every {
            authorizationService.filterEntitiesUserCanUpdate(any(), sub)
        } returns updatedEntitiesIds
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            listOf(nonExistingEntity)
        )
        every { entityOperationService.update(any()) } returns BatchOperationResult(
            success = mutableListOf(), errors = mutableListOf()
        )

        webClient.post()
            .uri(batchUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                    {
                        "errors": [
                            {
                                "entityId": "urn:ngsi-ld:Device:HCMR-AQUABOX200",
                                "error": [
                                    "Entity does not exist"
                                ]
                            }
                        ],
                        "success": []
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `create batch entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        val capturedExpandedEntities = slot<List<NgsiLdEntity>>()
        val capturedEntitiesIds = mutableListOf<URI>()
        val capturedEntityType = slot<String>()

        every { authorizationService.userCanCreateEntities(sub) } returns true
        every { entityOperationService.splitEntitiesByExistence(capture(capturedExpandedEntities)) } returns Pair(
            emptyList(),
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            entitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every { authorizationService.createAdminLink(any(), eq(sub)) } just Runs
        every {
            entityEventService.publishEntityCreateEvent(
                capture(capturedEntitiesIds), capture(capturedEntityType), any(), any()
            )
        } just Runs

        webClient.post()
            .uri(batchCreateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$[*]").isEqualTo(entitiesIds.map { it.toString() })

        assertEquals(entitiesIds, capturedExpandedEntities.captured.map { it.id })

        verify { authorizationService.createAdminLinks(entitiesIds, sub) }
        verify(timeout = 1000, exactly = 3) {
            entityEventService.publishEntityCreateEvent(any(), any(), any(), any())
        }
        capturedEntitiesIds.forEach { assertTrue(it in entitiesIds) }
        assertTrue(capturedEntityType.captured in listOf(sensorType, deviceType))
        confirmVerified()
    }

    @Test
    fun `create batch entity should return a 207 when some entities already exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        val existingEntity = mockk<NgsiLdEntity>()
        val capturedEntitiesIds = mutableListOf<URI>()
        val capturedEntityType = slot<String>()

        every { existingEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()

        every { authorizationService.userCanCreateEntities(sub) } returns true
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(existingEntity),
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every {
            entityEventService.publishEntityCreateEvent(
                capture(capturedEntitiesIds), capture(capturedEntityType), any(), any()
            )
        } just Runs

        webClient.post()
            .uri(batchCreateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(batchSomeEntitiesExistsResponse)

        verify { authorizationService.createAdminLinks(createdEntitiesIds, sub) }
        verify(timeout = 1000, exactly = 2) { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) }
        capturedEntitiesIds.forEach { assertTrue(it in createdEntitiesIds) }
        assertTrue(capturedEntityType.captured in listOf(sensorType, deviceType))
        confirmVerified()
    }

    @Test
    fun `create batch entity should not authorize user without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { authorizationService.userCanCreateEntities(sub) } returns false

        webClient.post()
            .uri(batchCreateEndpoint)
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

        verify { entityEventService wasNot called }
        confirmVerified()
    }

    @Test
    fun `create batch entity should return a 400 if JSON-LD payload is not correct`() {
        every { authorizationService.userCanCreateEntities(sub) } returns true
        shouldReturn400WithBadPayload("create")
    }

    @Test
    fun `create batch entity should return a 400 if one JSON-LD entity misses a context`() {
        every { authorizationService.userCanCreateEntities(sub) } returns true
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_one_entity_missing_context.json")
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":
                "One or more entities do not contain an @context and the request Content-Type is application/ld+json"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `upsert batch entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf("urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri())
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        val createdBatchResult = BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        val mockedCreatedEntity = mockkClass(NgsiLdEntity::class)

        every { mockedCreatedEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            listOf(mockedCreatedEntity)
        )
        every { authorizationService.userCanCreateEntities(sub) } returns true
        every { entityOperationService.create(any()) } returns createdBatchResult
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), sub)
        } returns emptyList()
        every { entityOperationService.update(any(), any()) } returns upsertUpdateBatchOperationResult
        every { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } just Runs
        every { entityEventService.publishAttributeAppendEvents(any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile).exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$[*]").isEqualTo(createdEntitiesIds.map { it.toString() })

        verify { authorizationService.createAdminLinks(createdEntitiesIds, sub) }
        verify {
            entityEventService.publishEntityCreateEvent(
                match { it in createdEntitiesIds },
                eq(sensorType),
                any(),
                eq(hcmrContext)
            )
        }
        verify(timeout = 1000, exactly = 2) {
            entityEventService.publishAttributeAppendEvents(
                match { it in entitiesIds },
                any(),
                match { it in upsertUpdateBatchOperationResult.success.map { it.updateResult } },
                hcmrContext
            )
        }
        confirmVerified()
    }

    @Test
    fun `upsert batch entity should return a 204 if it has only updated existing entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        val mockedCreatedEntity = mockkClass(NgsiLdEntity::class)

        every { mockedCreatedEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedCreatedEntity),
            emptyList()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(any(), sub)
        } returns entitiesIds
        every {
            entityOperationService.update(any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty
    }

    @Test
    fun `upsert batch entity should return a 207 if JSON-LD payload contains update errors`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")
        val errors = arrayListOf(
            BatchEntityError(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
                arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.")
            ),
            BatchEntityError(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
                arrayListOf("Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist.")
            )
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            emptyList()
        )
        every { authorizationService.userCanCreateEntities(sub) } returns true
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), sub)
        } returns emptyList()
        every { entityOperationService.update(any(), any()) } returns BatchOperationResult(
            arrayListOf(),
            errors
        )

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(batchUpsertOrUpdateWithUpdateErrorsResponse)

        verify { authorizationService.createAdminLinks(emptyList(), sub) }
        verify { entityEventService wasNot called }
        confirmVerified()
    }

    @Test
    fun `upsert batch entity without option should replace existing entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        val existingEntities = emptyList<NgsiLdEntity>()

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            emptyList()
        )
        every { authorizationService.userCanCreateEntities(sub) } returns true
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), sub)
        } returns emptyList()
        every { entityOperationService.replace(existingEntities) } returns BatchOperationResult(
            entitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri(batchUpsertEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityOperationService.replace(existingEntities) }
        verify { entityOperationService.update(any(), any()) wasNot Called }
        verify { authorizationService.createAdminLinks(emptyList(), sub) }
        verify(timeout = 1000, exactly = 2) {
            entityEventService.publishEntityReplaceEvent(
                match { it in entitiesIds },
                eq(sensorType),
                any(),
                eq(hcmrContext)
            )
        }
        confirmVerified()
    }

    @Test
    fun `upsert batch should not authorize user to create entities without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val expandedEntity = mockk<NgsiLdEntity>()
        every { expandedEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
        val nonExistingEntities = listOf(expandedEntity)

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            nonExistingEntities
        )
        every { authorizationService.userCanCreateEntities(sub) } returns false
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), sub)
        } returns emptyList()
        every { entityOperationService.replace(emptyList()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )

        webClient.post()
            .uri(batchUpsertEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
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
        verify { entityEventService wasNot called }
    }

    @Test
    fun `upsert batch should not authorize user to updates entities without write right`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesId = listOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
        )

        val expandedEntity = mockk<NgsiLdEntity>()
        every { expandedEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
        val expandedEntity2 = mockk<NgsiLdEntity>()
        every { expandedEntity2.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
        val existingEntities = listOf(expandedEntity, expandedEntity2)

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            emptyList()
        )
        val entitiesIdToUpdate = listOf(expandedEntity.id)
        every {
            authorizationService.filterEntitiesUserCanUpdate(
                entitiesId,
                sub
            )
        } returns entitiesIdToUpdate
        every { entityOperationService.replace(listOf(expandedEntity)) } returns BatchOperationResult(
            ArrayList(entitiesIdToUpdate.map { BatchEntitySuccess(it) }),
            arrayListOf()
        )

        webClient.post()
            .uri(batchUpsertEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(batchUpsertWithoutWriteRightResponse)

        verify { authorizationService.createAdminLinks(emptyList(), sub) }
        verify {
            entityEventService.publishEntityReplaceEvent(
                match { it in entitiesIdToUpdate },
                eq(sensorType),
                any(),
                eq(hcmrContext)
            )
        }
        confirmVerified()
    }

    @Test
    fun `upsert batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("upsert")
    }

    private fun shouldReturn400WithBadPayload(method: String) {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_missing_context.json")
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/$method")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":
                "Request payload must contain @context term for a request having an application/ld+json content type"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete batch for correct entities should return a 204`() {
        val entitiesIds = slot<List<URI>>()
        val deletedEntitiesIds = listOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            deviceAquaBox1.toUri()
        )
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(
                entitiesIds.captured,
                emptyList()
            )
        }
        val canAdminEntitiesIds = slot<List<URI>>()
        every { authorizationService.splitEntitiesByUserCanAdmin(capture(canAdminEntitiesIds), any()) } answers {
            Pair(
                canAdminEntitiesIds.captured,
                emptyList()
            )
        }

        val computedEntitiesIdsToDelete = slot<Set<URI>>()
        every { entityOperationService.delete(capture(computedEntitiesIdsToDelete)) } returns
            BatchOperationResult(
                deletedEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
                mutableListOf()
            )

        val entityIdToDelete = slot<URI>()
        every { entityOperationService.getEntityCoreProperties(capture(entityIdToDelete)) } answers {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns entityIdToDelete.captured
                every { type } returns listOf("Sensor")
                every { contexts } returns listOf(aquacContext!!)
            }
        }
        every { entityEventService.publishEntityDeleteEvent(any(), any(), any()) } just Runs

        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")
        webClient.post()
            .uri(batchDeleteEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify(timeout = 1000, exactly = 3) {
            entityEventService.publishEntityDeleteEvent(
                match { it in deletedEntitiesIds },
                eq("Sensor"),
                eq(listOf(aquacContext!!))
            )
        }
    }

    @Test
    fun `delete batch for unknown entities should return a 207 with explicit error messages`() {
        val entitiesIds = slot<List<URI>>()
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(
                emptyList(),
                entitiesIds.captured
            )
        }
        val existingEntitiesIds = slot<List<URI>>()
        every { authorizationService.splitEntitiesByUserCanAdmin(capture(existingEntitiesIds), any()) } answers {
            Pair(
                emptyList(),
                emptyList()
            )
        }

        val computedEntitiesIdsToDelete = slot<Set<URI>>()
        every { entityOperationService.delete(capture(computedEntitiesIdsToDelete)) } answers {
            BatchOperationResult(
                computedEntitiesIdsToDelete.captured.map { BatchEntitySuccess(it) }.toMutableList(),
                mutableListOf()
            )
        }

        performBatchDeleteAndCheck204Response("Entity does not exist")

        assertEquals(emptyList<String>(), existingEntitiesIds.captured)
        assertEquals(emptySet<String>(), computedEntitiesIdsToDelete.captured)

        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete batch for unauthorized entities should return a 207 with explicit error messages`() {
        val entitiesIds = slot<List<URI>>()
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(
                entitiesIds.captured,
                emptyList()
            )
        }
        val existingEntitiesIds = slot<List<URI>>()
        every { authorizationService.splitEntitiesByUserCanAdmin(capture(existingEntitiesIds), any()) } answers {
            Pair(
                emptyList(),
                existingEntitiesIds.captured
            )
        }

        val computedEntitiesIdsToDelete = slot<Set<URI>>()
        every { entityOperationService.delete(capture(computedEntitiesIdsToDelete)) } answers {
            BatchOperationResult(
                computedEntitiesIdsToDelete.captured.map { BatchEntitySuccess(it) }.toMutableList(),
                mutableListOf()
            )
        }

        performBatchDeleteAndCheck204Response("User forbidden to delete entity")

        assertEquals(emptySet<String>(), computedEntitiesIdsToDelete.captured)
        verify { entityEventService wasNot called }
    }

    private fun performBatchDeleteAndCheck204Response(expectedErrorMessage: String) {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")
        webClient.post()
            .uri(batchDeleteEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "success": [],
                    "errors": [
                        {"entityId":"urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                            "error":["$expectedErrorMessage"]},
                        {"entityId":"urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                            "error":["$expectedErrorMessage"]},
                        {"entityId":"urn:ngsi-ld:Device:HCMR-AQUABOX1",
                            "error":["$expectedErrorMessage"]}
                    ]
                }
                """.trimIndent()
            )
    }
}
