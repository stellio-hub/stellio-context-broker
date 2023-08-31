package com.egm.stellio.search.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityOperationService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpStatus
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.net.URI

@OptIn(ExperimentalCoroutinesApi::class)
@AutoConfigureWebTestClient(timeout = "30000")
@ActiveProfiles("test")
@WebFluxTest(EntityOperationHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class EntityOperationHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityOperationService: EntityOperationService

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    private val logger = LoggerFactory.getLogger(javaClass)

    private lateinit var mockedTemperatureSensorEntity: NgsiLdEntity
    private lateinit var mockedDissolvedOxygenSensorEntity: NgsiLdEntity
    private lateinit var mockedDeviceEntity: NgsiLdEntity
    private lateinit var mockedTemperatureSensorJsonLdEntity: JsonLdEntity
    private lateinit var mockedDissolvedOxygenSensorJsonLdEntity: JsonLdEntity
    private lateinit var mockedDeviceJsonLdEntity: JsonLdEntity

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
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
            every { types } returns listOf(SENSOR_TYPE)
            every { contexts } returns listOf(AQUAC_COMPOUND_CONTEXT)
        }
        mockedTemperatureSensorJsonLdEntity = mockkClass(JsonLdEntity::class) {
            every { id } returns temperatureSensorUri.toString()
            every { members } returns emptyMap()
        }
        mockedDissolvedOxygenSensorEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns dissolvedOxygenSensorUri
            every { types } returns listOf(SENSOR_TYPE)
            every { contexts } returns listOf(AQUAC_COMPOUND_CONTEXT)
        }
        mockedDissolvedOxygenSensorJsonLdEntity = mockkClass(JsonLdEntity::class) {
            every { id } returns dissolvedOxygenSensorUri.toString()
            every { members } returns emptyMap()
        }
        mockedDeviceEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns deviceUri
            every { types } returns listOf(DEVICE_TYPE)
            every { contexts } returns listOf(AQUAC_COMPOUND_CONTEXT)
        }
        mockedDeviceJsonLdEntity = mockkClass(JsonLdEntity::class) {
            every { id } returns deviceUri.toString()
            every { members } returns emptyMap()
        }
    }

    private val temperatureSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
    private val dissolvedOxygenSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
    private val deviceUri = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
    private val allEntitiesUris = listOf(temperatureSensorUri, dissolvedOxygenSensorUri, deviceUri)

    private val batchCreateEndpoint = "/ngsi-ld/v1/entityOperations/create"
    private val batchUpsertEndpoint = "/ngsi-ld/v1/entityOperations/upsert"
    private val batchUpsertWithUpdateEndpoint = "/ngsi-ld/v1/entityOperations/upsert?options=update"
    private val batchUpdateEndpoint = "/ngsi-ld/v1/entityOperations/update"
    private val batchDeleteEndpoint = "/ngsi-ld/v1/entityOperations/delete"

    private val hcmrContext = listOf(AQUAC_COMPOUND_CONTEXT)

    @Test
    fun `update batch entity should return a 204 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity, mockedDeviceEntity),
            emptyList()
        )
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityOperationService.update(any(), any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityOperationService.update(any(), false, eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"))
        }
    }

    @Test
    fun `update batch entity should return a 207 if JSON-LD payload contains update errors`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity),
            emptyList()
        )
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery { entityOperationService.update(any(), any(), any()) } returns BatchOperationResult(
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
    fun `update batch entity should return 204 if JSON-LD payload is correct and noOverwrite is asked`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity),
            emptyList()
        )
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityOperationService.update(any(), any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/update?options=noOverwrite")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityOperationService.update(any(), true, eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"))
        }
    }

    @Test
    fun `update batch entity should return 207 if there is a non existing entity in the payload`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity),
            listOf(mockedDeviceEntity)
        )
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery { entityOperationService.update(any(), any(), any()) } returns BatchOperationResult(
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
    fun `create batch entity should return a 201 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val capturedExpandedEntities = slot<List<NgsiLdEntity>>()
        val capturedEntitiesIds = mutableListOf<URI>()
        val capturedEntityTypes = slot<List<String>>()

        coEvery {
            entityOperationService.splitEntitiesByExistence(capture(capturedExpandedEntities))
        } answers { Pair(emptyList(), capturedExpandedEntities.captured) }
        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery { entityOperationService.create(any(), any(), any()) } returns BatchOperationResult(
            allEntitiesUris.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        coEvery { authorizationService.createAdminRights(any(), eq(sub)) } returns Unit.right()
        coEvery {
            entityEventService.publishEntityCreateEvent(
                any(),
                capture(capturedEntitiesIds),
                capture(capturedEntityTypes),
                any()
            )
        } returns Job()

        webClient.post()
            .uri(batchCreateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$[*]").isEqualTo(allEntitiesUris.map { it.toString() })

        assertEquals(allEntitiesUris, capturedExpandedEntities.captured.map { it.id })

        coVerify { authorizationService.createAdminRights(allEntitiesUris, sub) }
        coVerify(timeout = 1000, exactly = 3) {
            entityEventService.publishEntityCreateEvent(any(), any(), any(), any())
        }
        capturedEntitiesIds.forEach { assertTrue(it in allEntitiesUris) }
        assertTrue(capturedEntityTypes.captured[0] in listOf(SENSOR_TYPE, DEVICE_TYPE))
    }

    @Test
    fun `create batch entity should return a 207 when some entities already exist`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(dissolvedOxygenSensorUri, deviceUri)
        val capturedEntitiesIds = mutableListOf<URI>()
        val capturedEntityTypes = slot<List<String>>()

        coEvery {
            entityOperationService.splitEntitiesByExistence(any())
        } answers {
            Pair(
                listOf(mockedTemperatureSensorEntity),
                listOf(mockedDissolvedOxygenSensorEntity, mockedDeviceEntity)
            )
        }
        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery { entityOperationService.create(any(), any(), any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        coEvery { authorizationService.createAdminRights(any(), eq(sub)) } returns Unit.right()
        coEvery {
            entityEventService.publishEntityCreateEvent(
                any(),
                capture(capturedEntitiesIds),
                capture(capturedEntityTypes),
                any()
            )
        } returns Job()

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

        coVerify { authorizationService.createAdminRights(createdEntitiesIds, sub) }
        coVerify(timeout = 1000, exactly = 2) {
            entityEventService.publishEntityCreateEvent(any(), any(), any(), any())
        }
        capturedEntitiesIds.forEach { assertTrue(it in createdEntitiesIds) }
        assertTrue(capturedEntityTypes.captured[0] in listOf(SENSOR_TYPE, DEVICE_TYPE))
    }

    @Test
    fun `create batch entity should not authorize user without creator role`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        coEvery {
            entityOperationService.splitEntitiesByExistence(any())
        } returns Pair(emptyList(), listOf(mockedDeviceEntity))
        coEvery {
            authorizationService.userCanCreateEntities(sub)
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
    fun `create batch entity should return a 400 if JSON-LD payload is not correct`() = runTest {
        shouldReturn400WithBadPayload("create")
    }

    @Test
    fun `create batch entity should return a 400 if one JSON-LD entity misses a context`() = runTest {
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
    fun `upsert batch entity should return a 201 if JSON-LD payload is correct`() = runTest {
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

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedDissolvedOxygenSensorEntity, mockedDeviceEntity),
            listOf(mockedTemperatureSensorEntity)
        )
        coEvery { authorizationService.userCanCreateEntities(any()) } returns Unit.right()
        coEvery { entityOperationService.create(any(), any(), any()) } returns createdBatchResult
        coEvery { authorizationService.createAdminRights(any(), eq(sub)) } returns Unit.right()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } returns Job()

        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery { entityOperationService.update(any(), any(), any()) } returns updatedBatchResult
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any())
        } returns Job()

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile).exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$[*]").isEqualTo(createdEntitiesIds.map { it.toString() })

        coVerify { authorizationService.createAdminRights(createdEntitiesIds, sub) }
        coVerify {
            entityEventService.publishEntityCreateEvent(
                eq(sub.value),
                match { it in createdEntitiesIds },
                eq(listOf(SENSOR_TYPE)),
                eq(hcmrContext)
            )
        }
        coVerify(timeout = 1000, exactly = 2) {
            entityEventService.publishAttributeChangeEvents(
                eq(sub.value),
                match { it in updatedEntitiesIds },
                any(),
                match { it in updatedBatchResult.success.map { it.updateResult } },
                true,
                hcmrContext
            )
        }
    }

    @Test
    fun `upsert batch entity should return a 204 if it has only updated existing entities`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity),
            emptyList()
        )
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityOperationService.update(any(), any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify { entityOperationService.replace(any(), any()) wasNot Called }
        coVerify { entityOperationService.update(any(), false, sub.getOrNull()) }
    }

    @Test
    fun `upsert batch entity should return a 207 if JSON-LD payload contains update errors`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedDissolvedOxygenSensorEntity, mockedTemperatureSensorEntity),
            listOf(mockedDeviceEntity)
        )
        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery { entityOperationService.create(any(), any(), any()) } returns BatchOperationResult(
            arrayListOf(BatchEntitySuccess(deviceUri, mockkClass(UpdateResult::class))),
            arrayListOf()
        )
        coEvery { authorizationService.createAdminRights(any(), eq(sub)) } returns Unit.right()

        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery { entityOperationService.update(any(), any(), any()) } returns BatchOperationResult(
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

        coVerify { authorizationService.createAdminRights(listOf(deviceUri), sub) }
        coVerify(exactly = 1) { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) }
    }

    @Test
    fun `upsert batch entity without option should replace existing entities`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(temperatureSensorUri, dissolvedOxygenSensorUri)

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedDissolvedOxygenSensorEntity, mockedTemperatureSensorEntity),
            emptyList()
        )
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery { entityOperationService.replace(any(), any()) } returns BatchOperationResult(
            entitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } returns Job()

        webClient.post()
            .uri(batchUpsertEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify { entityOperationService.create(any(), any(), any()) wasNot Called }
        coVerify { entityOperationService.replace(any(), sub.getOrNull()) }
        coVerify { entityOperationService.update(any(), any(), any()) wasNot Called }
        coVerify(timeout = 1000, exactly = 2) {
            entityEventService.publishEntityReplaceEvent(
                eq(sub.value),
                match { it in entitiesIds },
                eq(listOf(SENSOR_TYPE)),
                eq(hcmrContext)
            )
        }
    }

    @Test
    fun `upsert batch should not authorize user to create entities without creator role`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            listOf(mockedTemperatureSensorEntity)
        )
        coEvery {
            authorizationService.userCanCreateEntities(sub)
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

        coVerify { entityEventService wasNot called }
        coVerify { entityOperationService.replace(any(), any()) wasNot Called }
    }

    @Test
    fun `upsert batch should not authorize user to updates entities without write right`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")

        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(mockedTemperatureSensorEntity, mockedDissolvedOxygenSensorEntity),
            emptyList()
        )
        coEvery {
            authorizationService.userCanUpdateEntity(match { it == temperatureSensorUri }, sub)
        } returns Unit.right()
        coEvery {
            authorizationService.userCanUpdateEntity(match { it == dissolvedOxygenSensorUri }, sub)
        } returns AccessDeniedException(ENTITY_UPDATE_FORBIDDEN_MESSAGE).left()
        coEvery { entityOperationService.replace(any(), any()) } returns BatchOperationResult(
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

        coVerify { entityOperationService.replace(any(), sub.getOrNull()) }
        coVerify {
            entityEventService.publishEntityReplaceEvent(
                eq(sub.value),
                eq(temperatureSensorUri),
                eq(listOf(SENSOR_TYPE)),
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
    fun `delete batch for correct entities should return a 204`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")

        coEvery { entityOperationService.splitEntitiesIdsByExistence(any()) } answers {
            Pair(allEntitiesUris, emptyList())
        }
        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityOperationService.delete(any()) } returns
            BatchOperationResult(
                allEntitiesUris.map { BatchEntitySuccess(it) }.toMutableList(),
                mutableListOf()
            )

        coEvery { entityPayloadService.retrieve(any<List<URI>>()) } returns
            listOf(
                mockkClass(EntityPayload::class, relaxed = true) {
                    every { entityId } returns dissolvedOxygenSensorUri
                    every { types } returns listOf(SENSOR_TYPE)
                    every { contexts } returns listOf(AQUAC_COMPOUND_CONTEXT)
                },
                mockkClass(EntityPayload::class, relaxed = true) {
                    every { entityId } returns temperatureSensorUri
                    every { types } returns listOf(SENSOR_TYPE)
                    every { contexts } returns listOf(AQUAC_COMPOUND_CONTEXT)
                },
                mockkClass(EntityPayload::class, relaxed = true) {
                    every { entityId } returns deviceUri
                    every { types } returns listOf(DEVICE_TYPE)
                    every { contexts } returns listOf(AQUAC_COMPOUND_CONTEXT)
                }
            )
        coEvery { entityEventService.publishEntityDeleteEvent(any(), any(), any(), any()) } returns Job()

        webClient.post()
            .uri(batchDeleteEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify(timeout = 1000, exactly = 3) {
            entityEventService.publishEntityDeleteEvent(
                eq(sub.value),
                match { it in allEntitiesUris },
                match { it[0] in listOf(SENSOR_TYPE, DEVICE_TYPE) },
                eq(listOf(AQUAC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `delete batch for unknown entities should return a 207 with explicit error messages`() = runTest {
        coEvery { entityOperationService.splitEntitiesIdsByExistence(any()) } answers {
            Pair(emptyList(), allEntitiesUris)
        }
        coEvery { authorizationService.userCanAdminEntity(any(), any()) } answers { Unit.right() }

        performBatchDeleteAndCheck207Response(ENTITY_DOES_NOT_EXIST_MESSAGE)

        coVerify { entityOperationService.splitEntitiesIdsByExistence(allEntitiesUris) }
        coVerify { entityPayloadService wasNot called }
        coVerify { entityOperationService.delete(any()) wasNot Called }
        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete batch for unauthorized entities should return a 207 with explicit error messages`() = runTest {
        coEvery { entityOperationService.splitEntitiesIdsByExistence(any()) } answers {
            Pair(allEntitiesUris, emptyList())
        }
        coEvery { entityPayloadService.retrieve(any<List<URI>>()) } returns
            listOf(
                mockkClass(EntityPayload::class, relaxed = true) {
                    every { entityId } returns dissolvedOxygenSensorUri
                    every { types } returns listOf(SENSOR_TYPE)
                },
                mockkClass(EntityPayload::class, relaxed = true) {
                    every { entityId } returns temperatureSensorUri
                    every { types } returns listOf(SENSOR_TYPE)
                },
                mockkClass(EntityPayload::class, relaxed = true) {
                    every { entityId } returns deviceUri
                    every { types } returns listOf(DEVICE_TYPE)
                }
            )
        coEvery {
            authorizationService.userCanAdminEntity(any(), any())
        } returns AccessDeniedException(ENTITY_DELETE_FORBIDDEN_MESSAGE).left()

        performBatchDeleteAndCheck207Response(ENTITY_DELETE_FORBIDDEN_MESSAGE)

        coVerify { entityOperationService.delete(any()) wasNot Called }
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
