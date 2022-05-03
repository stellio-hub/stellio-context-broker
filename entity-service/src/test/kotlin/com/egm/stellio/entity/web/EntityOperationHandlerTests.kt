package com.egm.stellio.entity.web

import arrow.core.Some
import arrow.core.left
import arrow.core.right
import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
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

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    private val logger = LoggerFactory.getLogger(javaClass)

    private lateinit var mockedTemperatureSensorEntity: NgsiLdEntity
    private lateinit var mockedDissolvedOxygenSensorEntity: NgsiLdEntity
    private lateinit var mockedDeviceEntity: NgsiLdEntity

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

        mockedTemperatureSensorEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns temperatureSensorUri
        }
        mockedDissolvedOxygenSensorEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns dissolvedOxygenSensorUri
        }
        mockedDeviceEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns deviceUri
        }
    }

    private val sub = Some("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")

    private val temperatureSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
    private val dissolvedOxygenSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
    private val deviceUri = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
    private val allEntitiesUris = listOf(temperatureSensorUri, dissolvedOxygenSensorUri, deviceUri)

    private val sensorType = "https://ontology.eglobalmark.com/egm#Sensor"
    private val deviceType = "https://ontology.eglobalmark.com/egm#Device"

    private val batchCreateEndpoint = "/ngsi-ld/v1/entityOperations/create"
    private val batchUpsertEndpoint = "/ngsi-ld/v1/entityOperations/upsert"
    private val batchUpsertWithUpdateEndpoint = "/ngsi-ld/v1/entityOperations/upsert?options=update"
    private val batchUpdateEndpoint = "/ngsi-ld/v1/entityOperations/update"
    private val batchDeleteEndpoint = "/ngsi-ld/v1/entityOperations/delete"

    private val hcmrContext = listOf(
        NGSILD_EGM_CONTEXT,
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/aquac/jsonld-contexts/aquac.jsonld",
        NGSILD_CORE_CONTEXT
    )

    @Test
    fun `update batch entity should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity, mockedDeviceEntity),
            emptyList()
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every {
            entityOperationService.update(any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify {
            entityOperationService.update(
                listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity, mockedDeviceEntity),
                false
            )
        }
    }

    @Test
    fun `update batch entity should return a 207 if JSON-LD payload contains update errors`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity),
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
    }

    @Test
    fun `update batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("update")
    }

    @Test
    fun `update batch entity should return 204 if JSON-LD payload is correct and noOverwrite is asked`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity),
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

        verify {
            entityOperationService.update(
                listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity),
                true
            )
        }
    }

    @Test
    fun `update batch entity should return 207 if there is a non existing entity in the payload`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity),
            listOf(mockedDeviceEntity)
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.update(any()) } returns BatchOperationResult(
            success = mutableListOf(BatchEntitySuccess(temperatureSensorUri, mockkClass(UpdateResult::class))),
            errors = mutableListOf()
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
                                "entityId": "urn:ngsi-ld:Device:HCMR-AQUABOX1",
                                "error": [ "Entity does not exist" ]
                            }
                        ],
                        "success": [ "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature" ]
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `create batch entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val capturedExpandedEntities = slot<List<NgsiLdEntity>>()
        val capturedEntitiesIds = mutableListOf<URI>()
        val capturedEntityTypes = slot<List<String>>()

        every {
            entityOperationService.splitEntitiesByExistence(capture(capturedExpandedEntities))
        } answers { Pair(emptyList(), capturedExpandedEntities.captured) }
        every { authorizationService.isCreationAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            allEntitiesUris.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every { authorizationService.createAdminLinks(any(), eq(sub)) } just Runs
        every {
            entityEventService.publishEntityCreateEvent(
                any(), capture(capturedEntitiesIds), capture(capturedEntityTypes), any()
            )
        } just Runs

        webClient.post()
            .uri(batchCreateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$[*]").isEqualTo(allEntitiesUris.map { it.toString() })

        assertEquals(allEntitiesUris, capturedExpandedEntities.captured.map { it.id })

        verify { authorizationService.createAdminLinks(allEntitiesUris, sub) }
        verify(timeout = 1000, exactly = 3) {
            entityEventService.publishEntityCreateEvent(any(), any(), any(), any())
        }
        capturedEntitiesIds.forEach { assertTrue(it in allEntitiesUris) }
        assertTrue(capturedEntityTypes.captured[0] in listOf(sensorType, deviceType))
    }

    @Test
    fun `create batch entity should return a 207 when some entities already exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(dissolvedOxygenSensorUri, deviceUri)
        val capturedEntitiesIds = mutableListOf<URI>()
        val capturedEntityTypes = slot<List<String>>()

        every {
            entityOperationService.splitEntitiesByExistence(any())
        } answers {
            Pair(
                listOf(mockedTemperatureSensorEntity),
                listOf(mockedDissolvedOxygenSensorEntity, mockedDeviceEntity)
            )
        }
        every { authorizationService.isCreationAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every { authorizationService.createAdminLinks(any(), eq(sub)) } just Runs
        every {
            entityEventService.publishEntityCreateEvent(
                any(), capture(capturedEntitiesIds), capture(capturedEntityTypes), any()
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
                            "error": [ "Entity already exists" ]
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
        assertTrue(capturedEntityTypes.captured[0] in listOf(sensorType, deviceType))
    }

    @Test
    fun `create batch entity should not authorize user without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every {
            entityOperationService.splitEntitiesByExistence(any())
        } returns Pair(emptyList(), listOf(mockedDeviceEntity))
        every {
            authorizationService.isCreationAuthorized(any(), sub)
        } returns AccessDeniedException(ENTITIY_CREATION_FORBIDDEN_MESSAGE).left()

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
        val updatedBatchResult = BatchOperationResult(
            updatedEntitiesIds.map { BatchEntitySuccess(it, mockkClass(UpdateResult::class)) }.toMutableList()
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedDissolvedOxygenSensorEntity, mockedDeviceEntity),
            listOf(mockedTemperatureSensorEntity)
        )
        every { authorizationService.isCreationAuthorized(any(), any()) } returns Unit.right()
        every { entityOperationService.create(any()) } returns createdBatchResult
        every { authorizationService.createAdminLinks(any(), eq(sub)) } just Runs
        every { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } just Runs

        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.update(any(), any()) } returns updatedBatchResult
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
                eq(sub.value),
                match { it in createdEntitiesIds },
                eq(listOf(sensorType)),
                eq(hcmrContext)
            )
        }
        verify(timeout = 1000, exactly = 2) {
            entityEventService.publishAttributeAppendEvents(
                eq(sub.value),
                match { it in updatedEntitiesIds },
                any(),
                match { it in updatedBatchResult.success.map { it.updateResult } },
                hcmrContext
            )
        }
    }

    @Test
    fun `upsert batch entity should return a 204 if it has only updated existing entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity),
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

        verify { entityOperationService.create(any()) wasNot Called }
        verify { entityOperationService.update(listOf(mockedTemperatureSensorEntity), false) }
    }

    @Test
    fun `upsert batch entity should return a 207 if JSON-LD payload contains update errors`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedDissolvedOxygenSensorEntity, mockedTemperatureSensorEntity),
            listOf(mockedDeviceEntity)
        )
        every { authorizationService.isCreationAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(BatchEntitySuccess(deviceUri, mockkClass(UpdateResult::class))),
            arrayListOf()
        )
        every { authorizationService.createAdminLinks(any(), eq(sub)) } just Runs

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
                    "success": [ "urn:ngsi-ld:Device:HCMR-AQUABOX1" ] 
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(listOf(deviceUri), sub) }
        verify(exactly = 1) { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) }
    }

    @Test
    fun `upsert batch entity without option should replace existing entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(temperatureSensorUri, dissolvedOxygenSensorUri)

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedDissolvedOxygenSensorEntity, mockedTemperatureSensorEntity),
            emptyList()
        )
        every { authorizationService.isUpdateAuthorized(any(), sub) } returns Unit.right()
        every { entityOperationService.replace(any()) } returns BatchOperationResult(
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
        verify {
            entityOperationService.replace(listOf(mockedDissolvedOxygenSensorEntity, mockedTemperatureSensorEntity))
        }
        verify { entityOperationService.update(any(), any()) wasNot Called }
        verify(timeout = 1000, exactly = 2) {
            entityEventService.publishEntityReplaceEvent(
                eq(sub.value),
                match { it in entitiesIds },
                eq(listOf(sensorType)),
                eq(hcmrContext)
            )
        }
    }

    @Test
    fun `upsert batch should not authorize user to create entities without creator role`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            listOf(mockedTemperatureSensorEntity)
        )
        every {
            authorizationService.isCreationAuthorized(any(), sub)
        } returns AccessDeniedException(ENTITIY_CREATION_FORBIDDEN_MESSAGE).left()

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
                            "error": [ "User forbidden to create entity" ]
                        }
                    ],
                    "success": []
                }
                """.trimIndent()
            )

        verify { entityEventService wasNot called }
        verify { entityOperationService.replace(any()) wasNot Called }
    }

    @Test
    fun `upsert batch should not authorize user to updates entities without write right`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity),
            emptyList()
        )
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
        every { entityOperationService.replace(any()) } returns BatchOperationResult(
            mutableListOf(BatchEntitySuccess(temperatureSensorUri)),
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
                            "error": [ "User forbidden to modify entity" ]
                        }
                    ],
                    "success": [ "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature" ]
                }
                """.trimIndent()
            )

        verify { entityOperationService.replace(listOf(mockedTemperatureSensorEntity)) }
        verify {
            entityEventService.publishEntityReplaceEvent(
                eq(sub.value),
                eq(temperatureSensorUri),
                eq(listOf(sensorType)),
                eq(hcmrContext)
            )
        }
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
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")

        every { entityOperationService.splitEntitiesIdsByExistence(any()) } answers {
            Pair(allEntitiesUris, emptyList())
        }
        every { authorizationService.isAdminAuthorized(any(), any(), any()) } returns Unit.right()
        every { entityOperationService.delete(any()) } returns
            BatchOperationResult(
                allEntitiesUris.map { BatchEntitySuccess(it) }.toMutableList(),
                mutableListOf()
            )

        val entityIdToDelete = slot<URI>()
        every { entityOperationService.getEntityCoreProperties(capture(entityIdToDelete)) } answers {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns entityIdToDelete.captured
                every { types } returns listOf(sensorType)
                every { contexts } returns listOf(aquacContext!!)
            }
        }
        every { entityEventService.publishEntityDeleteEvent(any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri(batchDeleteEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify(timeout = 1000, exactly = 3) {
            entityEventService.publishEntityDeleteEvent(
                eq(sub.value),
                match { it in allEntitiesUris },
                eq(sensorType),
                eq(listOf(aquacContext!!))
            )
        }
    }

    @Test
    fun `delete batch for unknown entities should return a 207 with explicit error messages`() {
        every { entityOperationService.splitEntitiesIdsByExistence(any()) } answers {
            Pair(emptyList(), allEntitiesUris)
        }
        every { authorizationService.isAdminAuthorized(any(), any(), any()) } answers { Unit.right() }

        performBatchDeleteAndCheck207Response(ENTITY_DOES_NOT_EXIST_MESSAGE)

        verify { entityOperationService.splitEntitiesIdsByExistence(allEntitiesUris) }
        verify { entityOperationService.delete(any()) wasNot Called }
        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete batch for unauthorized entities should return a 207 with explicit error messages`() {
        every { entityOperationService.splitEntitiesIdsByExistence(any()) } answers {
            Pair(allEntitiesUris, emptyList())
        }
        every { entityOperationService.getEntityCoreProperties(any()) } answers {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns dissolvedOxygenSensorUri
                every { types } returns listOf(sensorType)
            }
        } andThenAnswer {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns temperatureSensorUri
                every { types } returns listOf(sensorType)
            }
        } andThenAnswer {
            mockkClass(Entity::class, relaxed = true) {
                every { id } returns deviceUri
                every { types } returns listOf(deviceType)
            }
        }
        every {
            authorizationService.isAdminAuthorized(any(), any(), any())
        } returns AccessDeniedException(ENTITY_DELETE_FORBIDDEN_MESSAGE).left()

        performBatchDeleteAndCheck207Response(ENTITY_DELETE_FORBIDDEN_MESSAGE)

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
