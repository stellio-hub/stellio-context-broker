package com.egm.stellio.search.entity.web

import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.service.EntityOperationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.test.runTest
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

@AutoConfigureWebTestClient
@ActiveProfiles("test")
@WebFluxTest(EntityOperationHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class EntityOperationHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityOperationService: EntityOperationService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    private val logger = LoggerFactory.getLogger(javaClass)

    private lateinit var mockedTemperatureSensorEntity: NgsiLdEntity
    private lateinit var mockedDissolvedOxygenSensorEntity: NgsiLdEntity
    private lateinit var mockedDeviceEntity: NgsiLdEntity
    private lateinit var mockedTemperatureSensorExpandedEntity: ExpandedEntity
    private lateinit var mockedDissolvedOxygenSensorExpandedEntity: ExpandedEntity
    private lateinit var mockedDeviceExpandedEntity: ExpandedEntity

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
        }
        mockedTemperatureSensorExpandedEntity = mockkClass(ExpandedEntity::class) {
            every { id } returns temperatureSensorUri.toString()
            every { members } returns emptyMap()
        }
        mockedDissolvedOxygenSensorEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns dissolvedOxygenSensorUri
            every { types } returns listOf(SENSOR_TYPE)
        }
        mockedDissolvedOxygenSensorExpandedEntity = mockkClass(ExpandedEntity::class) {
            every { id } returns dissolvedOxygenSensorUri.toString()
            every { members } returns emptyMap()
        }
        mockedDeviceEntity = mockkClass(NgsiLdEntity::class) {
            every { id } returns deviceUri
            every { types } returns listOf(DEVICE_TYPE)
        }
        mockedDeviceExpandedEntity = mockkClass(ExpandedEntity::class) {
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
    private val batchUpdateEndpointWithNoOverwriteOption = "/ngsi-ld/v1/entityOperations/update?options=noOverwrite"
    private val batchDeleteEndpoint = "/ngsi-ld/v1/entityOperations/delete"
    private val queryEntitiesEndpoint = "/ngsi-ld/v1/entityOperations/query"
    private val batchMergeEndpoint = "/ngsi-ld/v1/entityOperations/merge"

    private val validJsonFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file.json")
    private val twoEntityOneInvalidJsonLDFile = ClassPathResource("/ngsild/two_sensors_one_invalid.jsonld")
    private val missingContextJsonFile = ClassPathResource("/ngsild/hcmr/HCMR_test_file_missing_context.json")
    private val oneEntityMissingContextJsonFile =
        ClassPathResource("/ngsild/hcmr/HCMR_test_file_one_entity_missing_context.json")
    private val deleteAllJsonFile = ClassPathResource("/ngsild/hcmr/HCMR_test_delete_all_entities.json")

    @Test
    fun `update batch entity should return a 204 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = validJsonFile

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
        val jsonLdFile = validJsonFile
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )

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
    fun `update batch entity should return a 207 if one entity is an invalid NGSI-LD payload`() = runTest {
        val jsonLdFile = twoEntityOneInvalidJsonLDFile

        coEvery { entityOperationService.update(any(), any(), any()) } returns BatchOperationResult(
            mutableListOf(BatchEntitySuccess(temperatureSensorUri, EMPTY_UPDATE_RESULT)),
            mutableListOf()
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
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX2temperature", 
                            "error": [ "Unable to expand input payload" ] 
                        }
                    ], 
                    "success": [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature"
                    ] 
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
        val jsonLdFile = validJsonFile
        coEvery {
            entityOperationService.update(any(), any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpdateEndpointWithNoOverwriteOption)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityOperationService.update(any(), true, eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"))
        }
    }

    @Test
    fun `create batch entity should return a 201 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = validJsonFile

        coEvery { entityOperationService.create(any(), any()) } returns BatchOperationResult(
            allEntitiesUris.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )

        webClient.post()
            .uri(batchCreateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$[*]").isEqualTo(allEntitiesUris.map { it.toString() })
    }

    @Test
    fun `create batch entity should return a 207 when some creation are in errors`() = runTest {
        val jsonLdFile = validJsonFile
        val createdEntitiesIds = arrayListOf(dissolvedOxygenSensorUri, deviceUri)

        coEvery { entityOperationService.create(any(), any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            mutableListOf(BatchEntityError(temperatureSensorUri, mutableListOf(ENTITY_ALREADY_EXISTS_MESSAGE)))
        )

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
    }

    @Test
    fun `create batch entity should return a 400 if JSON-LD payload is not correct`() = runTest {
        shouldReturn400WithBadPayload("create")
    }

    @Test
    fun `create batch entity should return a 400 if one JSON-LD entity misses a context`() = runTest {
        val jsonLdFile = oneEntityMissingContextJsonFile
        webClient.post()
            .uri(batchCreateEndpoint)
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
        val jsonLdFile = validJsonFile
        val capturedExpandedEntities = slot<List<JsonLdNgsiLdEntity>>()
        val createdEntitiesIds = arrayListOf(temperatureSensorUri)
        val updatedEntitiesIds = arrayListOf(dissolvedOxygenSensorUri, deviceUri)
        val createdBatchResult = BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )
        val updatedBatchResult = BatchOperationResult(
            updatedEntitiesIds.map { BatchEntitySuccess(it, mockkClass(UpdateResult::class)) }.toMutableList()
        )

        coEvery {
            entityOperationService.splitEntitiesByUniqueness(capture(capturedExpandedEntities))
        } answers { Pair(capturedExpandedEntities.captured, emptyList()) }
        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(
                Pair(mockedDissolvedOxygenSensorExpandedEntity, mockedDissolvedOxygenSensorEntity),
                Pair(mockedDeviceExpandedEntity, mockedDeviceEntity)
            ),
            listOf(Pair(mockedTemperatureSensorExpandedEntity, mockedTemperatureSensorEntity))
        )
        coEvery { entityOperationService.create(any(), any()) } returns createdBatchResult

        coEvery { entityOperationService.update(any(), any(), any()) } returns updatedBatchResult

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile).exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$[*]").isEqualTo(createdEntitiesIds.map { it.toString() })
    }

    @Test
    fun `upsert batch entity should return a 204 if it has only updated existing entities`() = runTest {
        val jsonLdFile = validJsonFile
        val capturedExpandedEntities = slot<List<JsonLdNgsiLdEntity>>()

        coEvery {
            entityOperationService.splitEntitiesByUniqueness(capture(capturedExpandedEntities))
        } answers {
            Pair(capturedExpandedEntities.captured, emptyList())
        }
        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(Pair(mockedTemperatureSensorExpandedEntity, mockedTemperatureSensorEntity)),
            emptyList()
        )
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
        val jsonLdFile = validJsonFile
        val capturedExpandedEntities = slot<List<JsonLdNgsiLdEntity>>()
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )

        coEvery { entityOperationService.splitEntitiesByUniqueness(capture(capturedExpandedEntities)) } answers {
            Pair(capturedExpandedEntities.captured, emptyList())
        }
        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(
                Pair(mockedTemperatureSensorExpandedEntity, mockedTemperatureSensorEntity),
                Pair(mockedDissolvedOxygenSensorExpandedEntity, mockedDissolvedOxygenSensorEntity)
            ),
            listOf(Pair(mockedDeviceExpandedEntity, mockedDeviceEntity))
        )
        coEvery { entityOperationService.create(any(), any()) } returns BatchOperationResult(
            arrayListOf(BatchEntitySuccess(deviceUri, mockkClass(UpdateResult::class))),
            arrayListOf()
        )

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
    }

    @Test
    fun `upsert batch entity without option should replace existing entities`() = runTest {
        val jsonLdFile = validJsonFile
        val capturedExpandedEntities = slot<List<JsonLdNgsiLdEntity>>()
        val entitiesIds = arrayListOf(temperatureSensorUri, dissolvedOxygenSensorUri)

        coEvery { entityOperationService.splitEntitiesByUniqueness(capture(capturedExpandedEntities)) } answers {
            Pair(capturedExpandedEntities.captured, emptyList())
        }
        coEvery { entityOperationService.splitEntitiesByExistence(any()) } returns Pair(
            listOf(
                Pair(mockedTemperatureSensorExpandedEntity, mockedTemperatureSensorEntity),
                Pair(mockedDissolvedOxygenSensorExpandedEntity, mockedDissolvedOxygenSensorEntity)
            ),
            emptyList()
        )
        coEvery { entityOperationService.replace(any(), any()) } returns BatchOperationResult(
            entitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            arrayListOf()
        )

        webClient.post()
            .uri(batchUpsertEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify { entityOperationService.create(any(), any()) wasNot Called }
        coVerify { entityOperationService.replace(any(), sub.getOrNull()) }
        coVerify { entityOperationService.update(any(), any(), any()) wasNot Called }
    }

    @Test
    fun `upsert batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("upsert")
    }

    private fun shouldReturn400WithBadPayload(method: String) {
        val jsonLdFile = missingContextJsonFile
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
        val jsonLdFile = deleteAllJsonFile

        coEvery { entityOperationService.delete(any(), any()) } returns
            BatchOperationResult(
                allEntitiesUris.map { BatchEntitySuccess(it) }.toMutableList(),
                mutableListOf()
            )

        webClient.post()
            .uri(batchDeleteEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `delete batch with errors should return a 207 with explicit error messages`() = runTest {
        coEvery { entityOperationService.delete(any(), any()) } returns
            BatchOperationResult(
                mutableListOf(BatchEntitySuccess(temperatureSensorUri), BatchEntitySuccess(dissolvedOxygenSensorUri)),
                mutableListOf(
                    BatchEntityError(deviceUri, mutableListOf(ENTITY_DOES_NOT_EXIST_MESSAGE))
                )
            )

        val jsonLdFile = deleteAllJsonFile

        webClient.post()
            .uri(batchDeleteEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "success": ["urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature","urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen"],
                    "errors": [
                        {"entityId":"urn:ngsi-ld:Device:HCMR-AQUABOX1",
                            "error":["$ENTITY_DOES_NOT_EXIST_MESSAGE"]}
                    ]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `query entities should return a 200 if the query is correct`() = runTest {
        coEvery {
            entityQueryService.queryEntities(any(), any<Sub>())
        } returns Pair(emptyList<ExpandedEntity>(), 0).right()

        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "$BEEHIVE_TYPE"
                }],
                "attrs": ["attr1", "attr2"]
            }
        """.trimIndent()

        webClient.post()
            .uri("$queryEntitiesEndpoint?limit=10&offset=20")
            .bodyValue(query)
            .exchange()
            .expectStatus().isOk

        coVerify {
            entityQueryService.queryEntities(
                match {
                    it.paginationQuery.limit == 10 &&
                        it.paginationQuery.offset == 20 &&
                        it.typeSelection == BEEHIVE_TYPE &&
                        it.attrs == setOf("${NGSILD_DEFAULT_VOCAB}attr1", "${NGSILD_DEFAULT_VOCAB}attr2")
                },
                any<Sub>()
            )
        }
    }

    @Test
    fun `merge batch entity should return a 204 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = validJsonFile

        coEvery {
            entityOperationService.merge(any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchMergeEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityOperationService.merge(any(), eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"))
        }
    }

    @Test
    fun `merge batch entity should return a 207 if JSON-LD payload contains update errors`() = runTest {
        val jsonLdFile = validJsonFile
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, arrayListOf("Update unexpectedly failed.")),
            BatchEntityError(dissolvedOxygenSensorUri, arrayListOf("Update unexpectedly failed."))
        )

        coEvery { entityOperationService.merge(any(), any()) } returns BatchOperationResult(
            mutableListOf(),
            errors
        )

        webClient.post()
            .uri(batchMergeEndpoint)
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
    fun `merge batch entity should return a 207 if one entity is an invalid NGSI-LD payload`() = runTest {
        val jsonLdFile = twoEntityOneInvalidJsonLDFile

        coEvery { entityOperationService.merge(any(), any()) } returns BatchOperationResult(
            mutableListOf(BatchEntitySuccess(temperatureSensorUri, EMPTY_UPDATE_RESULT)),
            mutableListOf()
        )

        webClient.post()
            .uri(batchMergeEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                { 
                    "errors": [
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX2temperature", 
                            "error": [ "Unable to expand input payload" ] 
                        }
                    ], 
                    "success": [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature"
                    ] 
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge batch entity should return a 400 if JSON-LD payload is not correct`() {
        shouldReturn400WithBadPayload("merge")
    }

    @Test
    fun `merge batch entity should return 207 if there is a non existing entity in the payload`() = runTest {
        val jsonLdFile = validJsonFile

        coEvery { entityOperationService.merge(any(), any()) } returns BatchOperationResult(
            success = mutableListOf(BatchEntitySuccess(temperatureSensorUri, mockkClass(UpdateResult::class))),
            errors = mutableListOf(BatchEntityError(deviceUri, mutableListOf("Entity does not exist")))
        )

        webClient.post()
            .uri(batchMergeEndpoint)
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
}
