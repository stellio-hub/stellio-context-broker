package com.egm.stellio.entity.web

import arrow.core.Some
import arrow.core.left
import arrow.core.right
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
import com.egm.stellio.shared.util.ENTITY_UPDATE_FORBIDDEN_MESSAGE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
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

    private val logger = LoggerFactory.getLogger(javaClass)

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .entityExchangeResultConsumer {
                logger.warn(String(it.responseBody as? ByteArray ?: "Empty response body".toByteArray()))
            }
            .build()
    }

    private val sub = Some("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")

    private val temperatureSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
    private val dissolvedOxygenSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
    private val deviceUri = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()

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
                deviceUri,
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
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )
        val mockedUpdatedEntities = listOf(
            mockkClass(NgsiLdEntity::class) {
                every { id } returns dissolvedOxygenSensorUri
            },
            mockkClass(NgsiLdEntity::class) {
                every { id } returns temperatureSensorUri
            }
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            mockedUpdatedEntities,
            emptyList()
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.update(any(), any()) } returns BatchOperationResult(
            mutableListOf(),
            errors
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
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", 
                            "error": [ 
                                "Update unexpectedly failed." 
                            ] 
                        }, 
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", 
                            "error": [ 
                                "Update unexpectedly failed." 
                            ] 
                        }
                    ], 
                    "success": [] 
                }
                """.trimIndent()
            )
    }

    @Test
    fun `update batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("update")
    }

    @Test
    fun `update batch entity should return 204 if JSON-LD payload is correct and noOverwrite is asked`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val mockedUpdatedEntites = listOf(
            mockkClass(NgsiLdEntity::class) {
                every { id } returns temperatureSensorUri
            },
            mockkClass(NgsiLdEntity::class) {
                every { id } returns dissolvedOxygenSensorUri
            }
        )
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            mockedUpdatedEntites,
            emptyList()
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
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

        every {
            entityOperationService.splitEntitiesByExistence(capture(capturedExpandedEntities))
        } answers { Pair(emptyList(), capturedExpandedEntities.captured) }
        every { authorizationService.isCreationAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            entitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every { authorizationService.createAdminLink(any(), eq(sub)) } just Runs
        every {
            entityEventService.publishEntityCreateEvent(
                any(), capture(capturedEntitiesIds), capture(capturedEntityType), any()
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
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
        )
        val existingEntity = mockk<NgsiLdEntity>()
        val capturedEntitiesIds = mutableListOf<URI>()
        val capturedEntityType = slot<String>()
        val capturedExpandedEntities = slot<List<NgsiLdEntity>>()

        every { existingEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
        every {
            entityOperationService.splitEntitiesByExistence(capture(capturedExpandedEntities))
        } answers {
            Pair(
                listOf(capturedExpandedEntities.captured[0]),
                capturedExpandedEntities.captured.subList(1, 2)
            )
        }
        every { authorizationService.isCreationAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every {
            entityEventService.publishEntityCreateEvent(
                any(), capture(capturedEntitiesIds), capture(capturedEntityType), any()
            )
        } just Runs

        webClient.post()
            .uri(batchCreateEndpoint)
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
                                "Entity already exists"
                            ]
                        }
                    ],
                    "success": [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                        "urn:ngsi-ld:Device:HCMR-AQUABOX1"
                    ]
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(createdEntitiesIds, sub) }
        verify(timeout = 1000, exactly = 2) { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) }
        capturedEntitiesIds.forEach { assertTrue(it in createdEntitiesIds) }
        assertTrue(capturedEntityType.captured in listOf(sensorType, deviceType))
        confirmVerified()
    }

    @Test
    fun `create batch entity should not authorize user without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        val mockedCreatedEntity = mockkClass(NgsiLdEntity::class)
        every { mockedCreatedEntity.id } returns deviceUri
        every {
            entityOperationService.splitEntitiesByExistence(any())
        } returns Pair(emptyList(), listOf(mockedCreatedEntity))
        every {
            authorizationService.isCreationAuthorized(any(), sub)
        } returns AccessDeniedException("User forbidden to create entity").left()

        webClient.post()
            .uri(batchCreateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "success": [],
                    "errors": [
                      {
                        "entityId":"urn:ngsi-ld:Device:HCMR-AQUABOX1",
                        "error":["User forbidden to create entity"]
                      }
                    ]
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
                "Request payload must contain @context term for a request having an application/ld+json content type"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `upsert batch entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(temperatureSensorUri)
        val updatedEntitiesIds = arrayListOf(dissolvedOxygenSensorUri, deviceUri)
        val createdBatchResult = BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        val mockedCreatedEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns temperatureSensorUri
        }
        val mockedUpdatedEntities = listOf(
            mockkClass(NgsiLdEntity::class) {
                every { id } returns dissolvedOxygenSensorUri
            },
            mockkClass(NgsiLdEntity::class) {
                every { id } returns deviceUri
            }
        )
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            mockedUpdatedEntities,
            listOf(mockedCreatedEntity)
        )
        every { authorizationService.isCreationAuthorized(any(), any()) } returns Unit.right()
        every { entityOperationService.create(any()) } returns createdBatchResult
        every { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } just Runs

        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.update(any(), any()) } returns upsertUpdateBatchOperationResult
        every { entityEventService.publishAttributeAppendEvents(any(), any(), any(), any(), any()) } just Runs

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
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                match { it in createdEntitiesIds },
                eq(sensorType),
                eq(hcmrContext)
            )
        }
        verify(timeout = 1000, exactly = 2) {
            entityEventService.publishAttributeAppendEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                match { it in updatedEntitiesIds },
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

        val mockedCreatedEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns temperatureSensorUri
        }
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedCreatedEntity),
            emptyList()
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
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
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )
        val mockedCreatedEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns deviceUri
        }
        val mockedUpdatedEntities = listOf(
            mockkClass(NgsiLdEntity::class) {
                every { id } returns dissolvedOxygenSensorUri
            },
            mockkClass(NgsiLdEntity::class) {
                every { id } returns temperatureSensorUri
            }
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            mockedUpdatedEntities,
            listOf(mockedCreatedEntity)
        )
        every { authorizationService.isCreationAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.update(any(), any()) } returns BatchOperationResult(
            arrayListOf(),
            errors
        )

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                { 
                    "errors": [
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature", 
                            "error": [ "Update unexpectedly failed." ] 
                        }, 
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", 
                            "error": [ "Update unexpectedly failed." ] 
                        }
                    ], 
                    "success": [] 
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(emptyList(), sub) }
        verify { entityEventService wasNot called }
        confirmVerified()
    }

    @Test
    fun `upsert batch entity without option should replace existing entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")
        val entitiesIds = arrayListOf(temperatureSensorUri, dissolvedOxygenSensorUri)
        val mockedReplacedEntities = listOf(
            mockkClass(NgsiLdEntity::class) {
                every { id } returns dissolvedOxygenSensorUri
            },
            mockkClass(NgsiLdEntity::class) {
                every { id } returns temperatureSensorUri
            }
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            mockedReplacedEntities,
            emptyList()
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.replace(mockedReplacedEntities) } returns BatchOperationResult(
            entitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri(batchUpsertEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityOperationService.create(any()) wasNot Called }
        verify { entityOperationService.replace(mockedReplacedEntities) }
        verify { entityOperationService.update(any(), any()) wasNot Called }
        verify(timeout = 1000, exactly = 2) {
            entityEventService.publishEntityReplaceEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                match { it in entitiesIds },
                eq(sensorType),
                eq(hcmrContext)
            )
        }
        confirmVerified()
    }

    @Test
    fun `upsert batch should not authorize user to create entities without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val expandedEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns temperatureSensorUri
        }

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            listOf(expandedEntity)
        )
        every { authorizationService.isCreationAuthorized(any(), sub) } returns AccessDeniedException("").left()

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
                                "User forbidden to create entity"
                            ]
                        }
                    ],
                    "success": []
                }
                """.trimIndent()
            )

        verify { entityEventService wasNot called }
        verify { entityOperationService.replace(any()) wasNot Called }
        confirmVerified()
    }

    @Test
    fun `upsert batch should not authorize user to updates entities without write right`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        val expandedEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns temperatureSensorUri
        }
        val expandedEntity2 = mockkClass(NgsiLdEntity::class) {
            every { id } returns dissolvedOxygenSensorUri
        }

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(expandedEntity, expandedEntity2),
            emptyList()
        )
        val entitiesIdToUpdate = listOf(expandedEntity.id)
        every {
            authorizationService.isUpdateAuthorized(
                match { it.id == temperatureSensorUri },
                sub
            )
        } returns Unit.right()
        every {
            authorizationService.isUpdateAuthorized(
                match { it.id == dissolvedOxygenSensorUri },
                sub
            )
        } returns AccessDeniedException(ENTITY_UPDATE_FORBIDDEN_MESSAGE).left()
        every { entityOperationService.replace(listOf(expandedEntity)) } returns BatchOperationResult(
            ArrayList(entitiesIdToUpdate.map { BatchEntitySuccess(it) }),
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

        verify {
            entityEventService.publishEntityReplaceEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                match { it in entitiesIdToUpdate },
                eq(sensorType),
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
            deviceUri
        )
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(entitiesIds.captured, emptyList())
        }
        every { authorizationService.isAdminAuthorized(any(), any(), any()) } returns Unit.right()

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
        every { entityEventService.publishEntityDeleteEvent(any(), any(), any(), any()) } just Runs

        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")
        webClient.post()
            .uri(batchDeleteEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify(timeout = 1000, exactly = 3) {
            entityEventService.publishEntityDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
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
            Pair(emptyList(), entitiesIds.captured)
        }
        every { authorizationService.isAdminAuthorized(any(), any(), any()) } answers { Unit.right() }

        performBatchDeleteAndCheck207Response("Entity does not exist")

        verify { entityOperationService.delete(any()) wasNot Called }
        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete batch for unauthorized entities should return a 207 with explicit error messages`() {
        val entitiesIds = slot<List<URI>>()
        every { entityOperationService.splitEntitiesIdsByExistence(capture(entitiesIds)) } answers {
            Pair(entitiesIds.captured, emptyList())
        }
        every { entityOperationService.getEntityCoreProperties(any()) } answers {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
                every { type } returns listOf(sensorType)
            }
        } andThenAnswer {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
                every { type } returns listOf(sensorType)
            }
        } andThenAnswer {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
                every { type } returns listOf(deviceType)
            }
        }
        every { authorizationService.isAdminAuthorized(any(), any(), any()) } returns AccessDeniedException("").left()

        performBatchDeleteAndCheck207Response("User forbidden to delete entity")

        verify { entityOperationService.delete(any()) wasNot Called }
        verify { entityEventService wasNot called }
    }

    private fun performBatchDeleteAndCheck207Response(expectedErrorMessage: String) {
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
