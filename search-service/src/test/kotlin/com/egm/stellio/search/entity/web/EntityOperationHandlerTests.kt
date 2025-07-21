package com.egm.stellio.search.entity.web

import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.service.EntityOperationService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.ENTITY_ALREADY_EXISTS_MESSAGE
import com.egm.stellio.shared.util.ENTITY_DOES_NOT_EXIST_MESSAGE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockkClass
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
    }

    private val temperatureSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature".toUri()
    private val dissolvedOxygenSensorUri = "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen".toUri()
    private val deviceUri = "urn:ngsi-ld:Device:HCMR-AQUABOX1".toUri()
    private val allEntitiesUris = listOf(temperatureSensorUri, dissolvedOxygenSensorUri, deviceUri)

    private val batchCreateEndpoint = "/ngsi-ld/v1/entityOperations/create"
    private val batchUpsertWithUpdateEndpoint = "/ngsi-ld/v1/entityOperations/upsert?options=update"
    private val batchUpsertWithUpdateAndNoOverwriteEndpoint =
        "/ngsi-ld/v1/entityOperations/upsert?options=update,noOverwrite"
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
    fun `update batch entity should return a 204 if all entities have been updated`() = runTest {
        val jsonLdFile = validJsonFile

        coEvery {
            entityOperationService.update(any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityOperationService.update(any())
        }
    }

    @Test
    fun `update batch entity should return a 207 if some entities could not be updated`() = runTest {
        val jsonLdFile = validJsonFile
        val errors = arrayListOf(
            BatchEntityError(
                temperatureSensorUri,
                InternalErrorException("Update unexpectedly failed.").toProblemDetail()
            ),
            BatchEntityError(
                dissolvedOxygenSensorUri,
                InternalErrorException("Update unexpectedly failed.").toProblemDetail()
            )
        )

        coEvery { entityOperationService.update(any(), any()) } returns BatchOperationResult(
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
                            "error": {
                                "type":"https://uri.etsi.org/ngsi-ld/errors/InternalError",
                                "title":"Update unexpectedly failed."
                            }
                        }, 
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", 
                            "error": {
                                "type":"https://uri.etsi.org/ngsi-ld/errors/InternalError",
                                "title":"Update unexpectedly failed."
                            }
                        }
                    ], 
                    "success": [] 
                }
                """.trimIndent()
            )
    }

    @Test
    fun `update batch entity should return a 207 if one entity has an invalid NGSI-LD payload`() = runTest {
        val jsonLdFile = twoEntityOneInvalidJsonLDFile

        coEvery { entityOperationService.update(any(), any()) } returns BatchOperationResult(
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
                            "error": {
                                "type":"${ErrorType.BAD_REQUEST_DATA.type}",
                                "title":"Unable to expand input payload"
                            }
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
    fun `update batch entity should return a 207 if entities are missing a JSON-LD context`() {
        shouldReturn207WithBadRequestErrors("update")
    }

    @Test
    fun `update batch entity with noOverwrite should return 204 if all entities have been updated`() = runTest {
        val jsonLdFile = validJsonFile
        coEvery {
            entityOperationService.update(any(), any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchUpdateEndpointWithNoOverwriteOption)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityOperationService.update(any(), true)
        }
    }

    @Test
    fun `create batch entity should return a 201 if all entities have been created`() = runTest {
        val jsonLdFile = validJsonFile

        coEvery { entityOperationService.create(any()) } returns BatchOperationResult(
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
    fun `create batch entity should return a 207 if some entities could not be created`() = runTest {
        val jsonLdFile = validJsonFile
        val createdEntitiesIds = arrayListOf(dissolvedOxygenSensorUri, deviceUri)

        coEvery { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList(),
            mutableListOf(
                BatchEntityError(
                    temperatureSensorUri,
                    AlreadyExistsException(ENTITY_ALREADY_EXISTS_MESSAGE).toProblemDetail()
                )
            )
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
                            "error": {
                                "type":"${ErrorType.ALREADY_EXISTS.type}",
                                "title":"Entity already exists"
                            }
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
    fun `create batch entity should return a 207 if entities are missing a JSON-LD context`() = runTest {
        shouldReturn207WithBadRequestErrors("create")
    }

    @Test
    fun `create batch entity should return a 207 if one entity misses a JSON-LD context`() = runTest {
        val jsonLdFile = oneEntityMissingContextJsonFile
        val createdEntitiesIds = arrayListOf(temperatureSensorUri, deviceUri)

        coEvery { entityOperationService.create(any()) } returns BatchOperationResult(
            createdEntitiesIds.map { BatchEntitySuccess(it) }.toMutableList()
        )

        webClient.post()
            .uri(batchCreateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "success": [
                        "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                        "urn:ngsi-ld:Device:HCMR-AQUABOX1"
                    ],
                    "errors": [
                        {
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                            "error": {
                                "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                                "title": "Request payload must contain @context term for a request having an application/ld+json content type",
                                "status": 400
                            },
                            "registrationId": null
                        }
                    ]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `upsert batch entity should return a 201 if all entities have been created`() = runTest {
        val jsonLdFile = validJsonFile
        val createdEntitiesIds = arrayListOf(temperatureSensorUri)
        val updatedEntitiesIds = arrayListOf(dissolvedOxygenSensorUri, deviceUri)

        val updatedBatchResult = BatchOperationResult(
            updatedEntitiesIds.map { BatchEntitySuccess(it, mockkClass(UpdateResult::class)) }.toMutableList()
        )

        coEvery { entityOperationService.upsert(any(), any(), any()) } returns (
            updatedBatchResult to createdEntitiesIds
            )

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
        coEvery {
            entityOperationService.upsert(any(), any(), any())
        } returns (BatchOperationResult(success = mutableListOf(), errors = mutableListOf()) to emptyList())

        webClient.post()
            .uri(batchUpsertWithUpdateEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty
    }

    @Test
    fun `upsert batch entity should return a 207 if some entities could not be upserted`() = runTest {
        val jsonLdFile = validJsonFile
        val errorMessage = "Update unexpectedly failed."
        val errors = arrayListOf(
            BatchEntityError(
                temperatureSensorUri,
                InternalErrorException(errorMessage).toProblemDetail()
            ),
            BatchEntityError(
                dissolvedOxygenSensorUri,
                InternalErrorException(errorMessage).toProblemDetail()
            )
        )

        coEvery { entityOperationService.upsert(any(), any(), any()) } returns (
            BatchOperationResult(
                arrayListOf(BatchEntitySuccess(deviceUri, mockkClass(UpdateResult::class))),
                errors
            ) to emptyList()
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
                            "error": {
                                "type":"${ErrorType.INTERNAL_ERROR.type}",
                                "title":"$errorMessage"
                            }
                        }, 
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", 
                            "error": {
                                "type":"${ErrorType.INTERNAL_ERROR.type}",
                                "title":"$errorMessage"
                            }
                        }
                    ], 
                    "success": [ "urn:ngsi-ld:Device:HCMR-AQUABOX1" ] 
                }
                """.trimIndent()
            )
    }

    @Test
    fun `upsert batch entity should update entities without overwriting if update and noOverwrite options`() = runTest {
        coEvery {
            entityOperationService.upsert(any(), any(), any())
        } returns (BatchOperationResult(success = mutableListOf(), errors = mutableListOf()) to emptyList())

        webClient.post()
            .uri(batchUpsertWithUpdateAndNoOverwriteEndpoint)
            .bodyValue(validJsonFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify(exactly = 1) {
            entityOperationService.upsert(any(), true, true)
        }
    }

    @Test
    fun `upsert batch entity should return a 207 if entities are missing a JSON-LD context`() {
        shouldReturn207WithBadRequestErrors("upsert")
    }

    private fun shouldReturn207WithBadRequestErrors(method: String) {
        val jsonLdFile = missingContextJsonFile
        webClient.post()
            .uri("/ngsi-ld/v1/entityOperations/$method")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "success": [],
                    "errors": [
                        {
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature",
                            "error": {
                                "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                                "title": "Request payload must contain @context term for a request having an application/ld+json content type",
                                "status": 400
                            },
                            "registrationId": null
                        },
                        {
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen",
                            "error": {
                                "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                                "title": "Request payload must contain @context term for a request having an application/ld+json content type",
                                "status": 400
                            },
                            "registrationId": null
                        },
                        {
                            "entityId": "urn:ngsi-ld:Device:HCMR-AQUABOX1",
                            "error": {
                                "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                                "title": "Request payload must contain @context term for a request having an application/ld+json content type",
                                "status": 400
                            },
                            "registrationId": null
                        }
                    ]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete batch entity should return a 204 if all entities have been deleted`() = runTest {
        val jsonLdFile = deleteAllJsonFile

        coEvery { entityOperationService.delete(any()) } returns
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
    fun `delete batch entity should return a 207 if some entities could not be deleted`() = runTest {
        coEvery { entityOperationService.delete(any()) } returns
            BatchOperationResult(
                mutableListOf(BatchEntitySuccess(temperatureSensorUri), BatchEntitySuccess(dissolvedOxygenSensorUri)),
                mutableListOf(
                    BatchEntityError(
                        deviceUri,
                        ResourceNotFoundException(ENTITY_DOES_NOT_EXIST_MESSAGE).toProblemDetail()
                    )
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
                        {
                            "entityId":"urn:ngsi-ld:Device:HCMR-AQUABOX1",
                            "error": {
                                "type":"${ErrorType.RESOURCE_NOT_FOUND.type}",
                                "title":"$ENTITY_DOES_NOT_EXIST_MESSAGE"
                            }
                        }
                    ]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `query entities should return a 200 if the query is correct`() = runTest {
        coEvery {
            entityQueryService.queryEntities(any())
        } returns Pair(emptyList<ExpandedEntity>(), 0).right()

        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "$BEEHIVE_IRI"
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
                        it is EntitiesQueryFromPost &&
                        it.entitySelectors!![0].typeSelection == BEEHIVE_IRI &&
                        it.attrs == setOf("${NGSILD_DEFAULT_VOCAB}attr1", "${NGSILD_DEFAULT_VOCAB}attr2")
                }
            )
        }
    }

    @Test
    fun `merge batch entity should return a 204 if all entities have been merged`() = runTest {
        val jsonLdFile = validJsonFile

        coEvery {
            entityOperationService.merge(any())
        } returns BatchOperationResult(success = mutableListOf(), errors = mutableListOf())

        webClient.post()
            .uri(batchMergeEndpoint)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityOperationService.merge(any())
        }
    }

    @Test
    fun `merge batch entity should return a 207 if some entities could not be merged`() = runTest {
        val jsonLdFile = validJsonFile
        val internalError = InternalErrorException("Update unexpectedly failed.").toProblemDetail()
        val errors = arrayListOf(
            BatchEntityError(temperatureSensorUri, internalError),
            BatchEntityError(dissolvedOxygenSensorUri, internalError)
        )

        coEvery { entityOperationService.merge(any()) } returns BatchOperationResult(
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
                            "error": {
                                "type":"${ErrorType.INTERNAL_ERROR.type}",
                                "title":"Update unexpectedly failed."
                            }
                        }, 
                        { 
                            "entityId": "urn:ngsi-ld:Sensor:HCMR-AQUABOX1dissolvedOxygen", 
                            "error": {
                                "type":"${ErrorType.INTERNAL_ERROR.type}",
                                "title":"Update unexpectedly failed."
                            }
                        }
                    ], 
                    "success": [] 
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge batch entity should return a 207 if one entity has an invalid NGSI-LD payload`() = runTest {
        val jsonLdFile = twoEntityOneInvalidJsonLDFile

        coEvery { entityOperationService.merge(any()) } returns BatchOperationResult(
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
                            "error": {
                                "type":"${ErrorType.BAD_REQUEST_DATA.type}",
                                "title":"Unable to expand input payload"
                            }
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
    fun `merge batch entity should return a 207 if entities are missing a JSON-LD context`() {
        shouldReturn207WithBadRequestErrors("merge")
    }

    @Test
    fun `merge batch entity should return 207 if there is a non existing entity in the payload`() = runTest {
        val jsonLdFile = validJsonFile

        coEvery { entityOperationService.merge(any()) } returns BatchOperationResult(
            success = mutableListOf(BatchEntitySuccess(temperatureSensorUri, mockkClass(UpdateResult::class))),
            errors = mutableListOf(
                BatchEntityError(
                    deviceUri,
                    ResourceNotFoundException("Entity does not exist").toProblemDetail()
                )
            )
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
                            "error": {
                                "type":"${ErrorType.RESOURCE_NOT_FOUND.type}",
                                "title":"Entity does not exist"
                            }
                        }
                    ],
                    "success": [ "urn:ngsi-ld:Sensor:HCMR-AQUABOX1temperature" ]
                }
                """.trimIndent()
            )
    }
}
