package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.NotUpdatedAttributeDetails
import com.egm.stellio.entity.model.UpdateAttributesResult
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdatedAttributeDetails
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityOperationService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.net.URI

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

    @MockkBean(relaxed = true)
    private lateinit var entityHandler: EntityHandler

    @MockkBean
    private lateinit var entityEventService: EntityEventService

    private val deviceAquaBox1 = "urn:ngsi-ld:Device:HCMR-AQUABOX1"
    private val deviceParameterAttribute = "https://ontology.eglobalmark.com/aquac#deviceParameter"

    private val batchFullSuccessResponse =
        """
        {
            "errors": [],
            "success": [
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                "$deviceAquaBox1"
            ]
        }
        """.trimIndent()

    private val batchUpsertFullSuccessResponse =
        """
        {
            "errors": [],
            "success": [
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen"
            ]
        }
        """.trimIndent()

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
                "$deviceAquaBox1"
            ]
        }
        """.trimIndent()

    private val hcmrContext = listOf(
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/shared-jsonld-contexts/egm.jsonld",
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/aquac/jsonld-contexts/aquac.jsonld",
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    )

    private val upsertUpdateBatchOperationResult = BatchOperationResult(
        mutableListOf(
            BatchEntityUpdateSuccess(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
                UpdateAttributesResult(
                    listOf(
                        UpdatedAttributeDetails(
                            deviceParameterAttribute,
                            null,
                            UpdateOperationResult.APPENDED
                        )
                    ),
                    emptyList()
                )
            ),
            BatchEntityUpdateSuccess(
                deviceAquaBox1.toUri(),
                UpdateAttributesResult(
                    listOf(
                        UpdatedAttributeDetails(
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

    private val upsertUpdateWithErrorsBatchOperationResult = BatchOperationResult(
        mutableListOf(
            BatchEntityUpdateSuccess(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
                UpdateAttributesResult(
                    emptyList(),
                    listOf(
                        NotUpdatedAttributeDetails(
                            deviceParameterAttribute,
                            "Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist."
                        )
                    )
                )
            ),
            BatchEntityUpdateSuccess(
                "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
                UpdateAttributesResult(
                    emptyList(),
                    listOf(
                        NotUpdatedAttributeDetails(
                            deviceParameterAttribute,
                            "Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist."
                        )
                    )
                )
            )
        ),
        mutableListOf()
    )

    @Test
    fun `create batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            deviceAquaBox1.toUri()
        )
        val expectedEntitiesPayload = getExpectedEntitiesEventsOperationPayload(
            jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8),
            entitiesIds
        )
        val expandedEntities = slot<List<NgsiLdEntity>>()
        val events = mutableListOf<EntityEvent>()
        val channelName = slot<String>()

        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityOperationService.splitEntitiesByExistence(capture(expandedEntities)) } returns Pair(
            emptyList(),
            emptyList()
        )
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            entitiesIds.map { BatchEntityCreateSuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every { authorizationService.createAdminLink(any(), eq("mock-user")) } just runs
        every {
            entityEventService.publishEntityEvent(capture(events), capture(channelName))
        } returns true as java.lang.Boolean

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(batchFullSuccessResponse)

        assertEquals(entitiesIds, expandedEntities.captured.map { it.id })

        verify { authorizationService.createAdminLinks(entitiesIds, "mock-user") }
        verify(timeout = 1000, exactly = 3) { entityEventService.publishEntityEvent(any(), any()) }
        events.forEach {
            it as EntityCreateEvent
            assertTrue(
                it.operationType == EventsType.ENTITY_CREATE &&
                    it.entityId in entitiesIds &&
                    it.operationPayload in expectedEntitiesPayload
            )
        }
        assertTrue(channelName.captured in listOf("Sensor", "Device"))
        confirmVerified()
    }

    @Test
    fun `create batch entity should return a 200 when some entities already exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            deviceAquaBox1.toUri()
        )
        val expectedEntitiesPayload = getExpectedEntitiesEventsOperationPayload(
            jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8),
            createdEntitiesIds
        )
        val existingEntity = mockk<NgsiLdEntity>()
        val events = mutableListOf<EntityEvent>()
        val channelName = slot<String>()

        every { existingEntity.id } returns "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()

        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(existingEntity),
            emptyList()
        )

        every { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntityCreateSuccess(it) }.toMutableList(),
            arrayListOf()
        )
        every {
            entityEventService.publishEntityEvent(capture(events), capture(channelName))
        } returns true as java.lang.Boolean

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/create")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(batchSomeEntitiesExistsResponse)

        verify { authorizationService.createAdminLinks(createdEntitiesIds, "mock-user") }
        verify(timeout = 1000, exactly = 2) { entityEventService.publishEntityEvent(any(), any()) }
        events.forEach {
            it as EntityCreateEvent
            assertTrue(
                it.operationType == EventsType.ENTITY_CREATE &&
                    it.entityId in createdEntitiesIds &&
                    it.operationPayload in expectedEntitiesPayload
            )
        }
        assertTrue(channelName.captured in listOf("Sensor", "Device"))
        confirmVerified()
    }

    @Test
    fun `create batch entity should not authorize user without creator role`() {
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

        verify { entityEventService wasNot called }
        confirmVerified()
    }

    @Test
    fun `create batch entity should return a 400 if JSON-LD payload is not correct`() {
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        shouldReturn400WithBadPayload("create")
    }

    @Test
    fun `upsert batch entity should return a 200 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
        val createdEntitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
        )
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            deviceAquaBox1.toUri()
        )
        val createdBatchResult = BatchOperationResult(
            createdEntitiesIds.map { BatchEntityCreateSuccess(it) }.toMutableList(),
            arrayListOf()
        )

        val existingEntities = emptyList<NgsiLdEntity>()
        val updatedEntitiesIds = slot<URI>()
        val updateAttributesResults = slot<UpdateAttributesResult>()

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            listOf(mockkClass(NgsiLdEntity::class))
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityOperationService.create(any()) } returns createdBatchResult
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user")
        } returns emptyList()
        every { entityOperationService.update(existingEntities) } returns upsertUpdateBatchOperationResult
        every {
            entityHandler.publishAppendEntityAttributesEvents(
                capture(updatedEntitiesIds), any(), capture(updateAttributesResults), any()
            )
        } just Runs
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert?options=update")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(batchFullSuccessResponse)

        verify { authorizationService.createAdminLinks(createdEntitiesIds, "mock-user") }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as EntityCreateEvent
                    it.operationType == EventsType.ENTITY_CREATE &&
                        it.entityId in createdEntitiesIds
                },
                "Sensor"
            )
        }
        verify(timeout = 1000, exactly = 2) {
            entityHandler.publishAppendEntityAttributesEvents(any(), any(), any(), hcmrContext)
        }
        assertTrue(updatedEntitiesIds.captured in entitiesIds)
        assertTrue(
            updateAttributesResults.captured in upsertUpdateBatchOperationResult.success.map {
                it as BatchEntityUpdateSuccess
                it.updateAttributesResult
            }
        )
        confirmVerified()
    }

    @Test
    fun `upsert batch entity should return a 200 if JSON-LD payload contains update errors`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            emptyList(),
            emptyList()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user")
        } returns emptyList()
        every { entityOperationService.update(any()) } returns upsertUpdateWithErrorsBatchOperationResult

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
                                "deviceParameter : Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist." 
                            ] 
                        }, 
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", 
                            "error": [ 
                                "deviceParameter : Target entity urn:ngsi-ld:Device:HCMR-AQUABOX2 does not exist." 
                            ] 
                        }
                    ], 
                    "success": [] 
                }
                """.trimIndent()
            )

        verify { authorizationService.createAdminLinks(emptyList(), "mock-user") }
        verify { entityEventService wasNot called }
        confirmVerified()
    }

    @Test
    fun `upsert batch entity without option should replace entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_invalid_relation_update.json")
        val entitiesIds = arrayListOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
        )
        val existingEntities = emptyList<NgsiLdEntity>()

        every { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            existingEntities,
            emptyList()
        )
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityOperationService.create(any()) } returns BatchOperationResult(
            arrayListOf(),
            arrayListOf()
        )
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user")
        } returns emptyList()
        every { entityOperationService.replace(existingEntities) } returns BatchOperationResult(
            entitiesIds.map { BatchEntityReplaceSuccess(it, emptyList()) }.toMutableList(),
            arrayListOf()
        )
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/upsert")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(batchUpsertFullSuccessResponse)

        verify { entityOperationService.replace(existingEntities) }
        verify { entityOperationService.update(any()) wasNot Called }
        verify { authorizationService.createAdminLinks(emptyList(), "mock-user") }
        verify(timeout = 1000, exactly = 2) {
            entityEventService.publishEntityEvent(
                match {
                    it as EntityReplaceEvent
                    it.operationType == EventsType.ENTITY_REPLACE &&
                        it.entityId in entitiesIds
                },
                "Sensor"
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
        every { authorizationService.userCanCreateEntities("mock-user") } returns false
        every {
            authorizationService.filterEntitiesUserCanUpdate(emptyList(), "mock-user")
        } returns emptyList()
        every { entityOperationService.replace(emptyList()) } returns BatchOperationResult(
            arrayListOf(),
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

        verify { entityHandler wasNot called }
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
                "mock-user"
            )
        } returns entitiesIdToUpdate
        every { entityOperationService.replace(listOf(expandedEntity)) } returns BatchOperationResult(
            ArrayList(entitiesIdToUpdate.map { BatchEntityReplaceSuccess(it, emptyList()) }.toMutableList()),
            arrayListOf()
        )
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

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
        verify(timeout = 1000, exactly = 1) {
            entityEventService.publishEntityEvent(
                match {
                    it as EntityReplaceEvent
                    it.operationType == EventsType.ENTITY_REPLACE &&
                        it.entityId == "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
                },
                "Sensor"
            )
        }
        confirmVerified()
    }

    @Test
    fun `upsert batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("upsert")
        verify { entityHandler wasNot called }
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
                "detail":"Unable to parse input payload"}
                """.trimIndent()
            )
    }

    @Test
    fun `delete batch for correct entities should return a 200 with explicit success message`() {
        val deletedEntitiesIds = listOf(
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri(),
            "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri(),
            deviceAquaBox1.toUri()
        )
        val entitiesIds = slot<List<URI>>()
        val channelName = slot<String>()
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
                deletedEntitiesIds.map { BatchEntityDeleteSuccess(it) }.toMutableList(),
                mutableListOf()
            )
        every { entityOperationService.getEntityType(deletedEntitiesIds[0]) } returns "Sensor"
        every { entityOperationService.getEntityType(deletedEntitiesIds[1]) } returns "Sensor"
        every { entityOperationService.getEntityType(deletedEntitiesIds[2]) } returns "Device"
        every { entityEventService.publishEntityEvent(any(), capture(channelName)) } returns true as java.lang.Boolean

        val jsonLdFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/delete")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(batchFullSuccessResponse)
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as EntityDeleteEvent
                    it.entityId in deletedEntitiesIds
                },
                any()
            )
        }
        assertTrue(channelName.captured in listOf("Sensor", "Device"))
    }

    @Test
    fun `delete batch for unknown entities should return a 200 with explicit error messages`() {
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
                computedEntitiesIdsToDelete.captured.map { BatchEntityDeleteSuccess(it) }.toMutableList(),
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
                    {"entityId":"$deviceAquaBox1","error":["Entity does not exist"]}
                ]
            }
                """.trimIndent()
            )
        assertEquals(emptyList<String>(), existingEntitiesIds.captured)
        assertEquals(emptySet<String>(), computedEntitiesIdsToDelete.captured)
        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete batch for unauthorized entities should return a 200 with explicit error messages`() {
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
                computedEntitiesIdsToDelete.captured.map { BatchEntityDeleteSuccess(it) }.toMutableList(),
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
            {"entityId":"$deviceAquaBox1","error":["User forbidden to delete entity"]}
        ]
    }
                """.trimIndent()
            )
        assertEquals(emptySet<String>(), computedEntitiesIdsToDelete.captured)
        verify { entityEventService wasNot called }
    }

    private fun getExpectedEntitiesEventsOperationPayload(
        entitiesPayload: String,
        createdEntitiesIds: List<URI> = emptyList()
    ) = JsonUtils.parseListOfEntities(entitiesPayload).filter { it["id"].toString().toUri() in createdEntitiesIds }
        .map { serializeObject(it) }
}
