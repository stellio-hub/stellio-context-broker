package com.egm.stellio.search.entity.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.service.DistributedEntityConsumptionService
import com.egm.stellio.search.csr.service.DistributedEntityProvisionService
import com.egm.stellio.search.entity.model.EMPTY_UPDATE_RESULT
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.NotUpdatedDetails
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.entity.service.LinkedEntityService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ConflictException
import com.egm.stellio.shared.model.ErrorType
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.model.NGSILD_DATE_TYPE
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.model.NGSILD_TIME_TYPE
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.AQUAC_HEADER_LINK
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.entityOrAttrsNotFoundMessage
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.called
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.hamcrest.core.Is
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.lang.reflect.UndeclaredThrowableException
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.ZonedDateTime

@ActiveProfiles("test")
@WebFluxTest(EntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class EntityHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @Autowired
    private lateinit var applicationProperties: ApplicationProperties

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var distributedEntityConsumptionService: DistributedEntityConsumptionService

    @MockkBean
    private lateinit var distributedEntityProvisionService: DistributedEntityProvisionService

    @MockkBean(relaxed = true)
    private lateinit var linkedEntityService: LinkedEntityService

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @BeforeEach
    fun mockNoCSR() {
        coEvery {
            distributedEntityConsumptionService
                .distributeRetrieveEntityOperation(any(), any(), any(), any())
        } returns (emptyList<NGSILDWarning>() to emptyList())
        coEvery {
            distributedEntityConsumptionService
                .distributeQueryEntitiesOperation(any(), any(), any())
        } returns Triple(emptyList(), emptyList(), emptyList())
        val capturedExpandedEntity = slot<ExpandedEntity>()
        coEvery {
            distributedEntityProvisionService
                .distributeCreateEntity(capture(capturedExpandedEntity), any())
        } answers { BatchOperationResult() to capturedExpandedEntity.captured }
        coEvery {
            distributedEntityProvisionService
                .distributeDeleteEntity(any(), any())
        } answers { BatchOperationResult() }
        coEvery {
            distributedEntityProvisionService
                .distributeReplaceEntity(capture(capturedExpandedEntity), any(), any())
        } answers { BatchOperationResult() to capturedExpandedEntity.captured }
    }

    private val beehiveId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
    private val fishSizeAttribute = "https://ontology.eglobalmark.com/aquac#fishSize"

    @Test
    fun `create entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.createEntity(any<NgsiLdEntity>(), any())
        } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/$breedingServiceId"))

        coVerify {
            entityService.createEntity(
                match<NgsiLdEntity> {
                    it.id == breedingServiceId
                },
                any()
            )
        }
    }

    @Test
    fun `create entity should return a 409 if the entity already exists`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        coEvery {
            entityService.createEntity(any(), any())
        } returns AlreadyExistsException("Already Exists").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(409)
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/AlreadyExists",
                      "title": "Already Exists"
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 500 error if there is an internal server error`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        coEvery {
            entityService.createEntity(any<NgsiLdEntity>(), any())
        } throws InternalErrorException("Internal Server Exception")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(500)
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/InternalError",
                      "title": "Internal Server Exception"
                    }
                    """
            )
    }

    @Test
    fun `create entity should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if entity is not NGSI-LD valid`() {
        val entityWithoutId =
            """
            {
                "type": "Beehive"
            }
            """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .bodyValue(entityWithoutId)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if entity does not have an type`() {
        val entityWithoutType =
            """
            {
                "id": "urn:ngsi-ld:Beehive:9876"
            }
            """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .bodyValue(entityWithoutType)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if creation unexpectedly fails`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        // reproduce the runtime behavior where the raised exception is wrapped in an UndeclaredThrowableException
        coEvery {
            entityService.createEntity(any<NgsiLdEntity>(), any())
        } throws UndeclaredThrowableException(BadRequestDataException("Target entity does not exist"))

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Target entity does not exist"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 403 if user is not allowed to create entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        coEvery {
            entityService.createEntity(any(), any())
        } returns AccessDeniedException("User forbidden to create entities").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden to create entities"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 400 if it contains an invalid query parameter`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities?invalid=invalid")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/InvalidRequest",
                    "title": "The ['invalid'] parameters are not allowed on this endpoint. Accepted query parameters are 'local'. "
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 501 if it contains a not implemented query parameter`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities?Via=true")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(501)
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/NotImplemented",
                    "title": "The ['Via'] parameters have not been implemented yet. Accepted query parameters are 'local'. "
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 207 if some contextSources failed`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val capturedExpandedEntity = slot<ExpandedEntity>()

        val error = BatchEntityError(
            entityId = "urn:ngsi-ld:BreedingService:0214".toUri(),
            error = ConflictException("error").toProblemDetail(),
            registrationId = "id:2".toUri()
        )
        val firstResult = BatchOperationResult(
            mutableListOf(BatchEntitySuccess("id:1".toUri())),
            mutableListOf(error)
        )
        coEvery {
            distributedEntityProvisionService
                .distributeCreateEntity(capture(capturedExpandedEntity), any())
        } answers {
            firstResult to capturedExpandedEntity.captured
        }
        coEvery {
            entityService.createEntity(any(), any())
        } returns AccessDeniedException("User forbidden to create entities").left()

        val expectedJson = """
                {
                    "success": ["id:1"],
                    "errors":[
                        {
                            "entityId":"urn:ngsi-ld:BreedingService:0214",
                            "error":{
                                "type": "https://uri.etsi.org/ngsi-ld/errors/Conflict",
                                "title": "error"
                            },
                            "registrationId": "id:2"
                        },
                        {
                            "entityId":"urn:ngsi-ld:BreedingService:0214",
                            "error":{
                                "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                                "title": "User forbidden to create entities"
                            },
                            "registrationId":null
                        }
                    ]
                }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(207)
            .expectBody().json(expectedJson)
    }

    @Test
    fun `create entity should return a 409 if a context source containing all the information returned a conflict`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val capturedExpandedEntity = slot<ExpandedEntity>()

        val error = BatchEntityError(
            entityId = "entity:1".toUri(),
            error = ConflictException("my test message").toProblemDetail(),
            registrationId = "i:2".toUri()
        )
        val result = BatchOperationResult(
            mutableListOf(),
            mutableListOf(error)
        )
        coEvery {
            distributedEntityProvisionService
                .distributeCreateEntity(capture(capturedExpandedEntity), any())
        } answers {
            result to null
        }
        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(409)
            .expectBody().json(
                """
                {
                    "type": "${ErrorType.CONFLICT.type}",
                    "title": "my test message"
                }
                """.trimIndent()
            )
    }

    fun initializeRetrieveEntityMocks() {
        val compactedEntity = slot<CompactedEntity>()

        coEvery {
            linkedEntityService.processLinkedEntities(capture(compactedEntity), any())
        } answers {
            listOf(compactedEntity.captured).right()
        }
    }

    @Test
    fun `get entity by id should return 200 when entity exists`() {
        initializeRetrieveEntityMocks()
        val returnedExpandedEntity = mockkClass(ExpandedEntity::class, relaxed = true)
        coEvery { entityQueryService.queryEntity(any()) } returns returnedExpandedEntity.right()
        every { returnedExpandedEntity.checkContainsAnyOf(any()) } returns Unit.right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `get entity by id should correctly serialize temporal properties`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                NGSILD_CREATED_AT_IRI to
                    mapOf(
                        "@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                    ),
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "createdAt": "2015-10-18T11:20:30.000001Z",
                    "@context": "${applicationProperties.contexts.core}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly filter the asked attributes`() = runTest {
        initializeRetrieveEntityMocks()
        val entity = """
            {
                "id": "$beehiveId",
                "type": "Beehive",
                "attr1": {
                    "type": "Property",
                    "value": "some value 1"
                },
                "attr2": {
                    "type": "Property",
                    "value": "some value 2"
                },
                "@context" : [
                     "http://localhost:8093/jsonld-contexts/apic-compound.jsonld"
                ]
            }
        """.trimIndent()
        val expandedEntity = expandJsonLdEntity(entity)

        coEvery { entityQueryService.queryEntity(any()) } returns expandedEntity.right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?attrs=attr2")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.attr1").doesNotExist()
            .jsonPath("$.attr2").isNotEmpty
    }

    @Test
    fun `get entity by id should correctly return the simplified representation of an entity`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive"),
                "https://uri.etsi.org/ngsi-ld/default-context/prop1" to mapOf(
                    JSONLD_TYPE_KW to NGSILD_PROPERTY_TYPE.uri,
                    NGSILD_PROPERTY_VALUE to mapOf(
                        JSONLD_VALUE_KW to "some value"
                    )
                ),
                "https://uri.etsi.org/ngsi-ld/default-context/rel1" to mapOf(
                    JSONLD_TYPE_KW to NGSILD_RELATIONSHIP_TYPE.uri,
                    NGSILD_RELATIONSHIP_OBJECT to mapOf(
                        JSONLD_ID_KW to "urn:ngsi-ld:Entity:1234"
                    )
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?options=keyValues")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .json(
                """
                {
                    "id": "$beehiveId",
                    "type": "Beehive",
                    "prop1": "some value",
                    "rel1": "urn:ngsi-ld:Entity:1234",
                    "@context": "${applicationProperties.contexts.core}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly return the representation asked in the format parameter`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive"),
                "https://uri.etsi.org/ngsi-ld/default-context/prop1" to mapOf(
                    JSONLD_TYPE_KW to NGSILD_PROPERTY_TYPE.uri,
                    NGSILD_PROPERTY_VALUE to mapOf(
                        JSONLD_VALUE_KW to "some value"
                    )
                ),
                "https://uri.etsi.org/ngsi-ld/default-context/rel1" to mapOf(
                    JSONLD_TYPE_KW to NGSILD_RELATIONSHIP_TYPE.uri,
                    NGSILD_RELATIONSHIP_OBJECT to mapOf(
                        JSONLD_ID_KW to "urn:ngsi-ld:Entity:1234"
                    )
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?format=keyValues&options=normalized")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .json(
                """
                {
                    "id": "$beehiveId",
                    "type": "Beehive",
                    "prop1": "some value",
                    "rel1": "urn:ngsi-ld:Entity:1234",
                    "@context": "${applicationProperties.contexts.core}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should return 404 if the entity has none of the requested attributes`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf(BEEHIVE_IRI)
            )
        ).right()

        val expectedMessage = entityOrAttrsNotFoundMessage(
            beehiveId,
            setOf("https://uri.etsi.org/ngsi-ld/default-context/attr2")
        )
        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?attrs=attr2")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "$expectedMessage"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should not include temporal properties if optional query param sysAttrs is not present`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("""{"@context":"${applicationProperties.contexts.core}"}""")
            .jsonPath("$.createdAt").doesNotExist()
            .jsonPath("$.modifiedAt").doesNotExist()
    }

    @Test
    fun `get entity by id should correctly serialize properties of type DateTime and display sysAttrs asked`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                NGSILD_CREATED_AT_IRI to
                    mapOf(
                        "@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                    ),
                "https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf(
                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
                    NGSILD_PROPERTY_VALUE to mapOf(
                        "@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                    ),
                    NGSILD_CREATED_AT_IRI to
                        mapOf(
                            "@type" to NGSILD_DATE_TIME_TYPE,
                            "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                        ),
                    NGSILD_MODIFIED_AT_IRI to
                        mapOf(
                            "@type" to NGSILD_DATE_TIME_TYPE,
                            "@value" to Instant.parse("2015-10-18T12:20:30.000001Z").atZone(ZoneOffset.UTC)
                        )
                ),
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "createdAt":"2015-10-18T11:20:30.000001Z",
                    "testedAt":{
                        "type": "Property",
                        "value":{
                            "type": "DateTime",
                            "@value":"2015-10-18T11:20:30.000001Z"
                        },
                        "createdAt":"2015-10-18T11:20:30.000001Z",
                        "modifiedAt":"2015-10-18T12:20:30.000001Z"
                    },
                    "@context": "${applicationProperties.contexts.core}"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Date`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf(
                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
                    NGSILD_PROPERTY_VALUE to mapOf(
                        "@type" to NGSILD_DATE_TYPE,
                        "@value" to LocalDate.of(2015, 10, 18)
                    )
                ),
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "testedAt":{
                        "type": "Property",
                        "value":{
                            "type": "Date",
                            "@value":"2015-10-18"
                        }
                    },
                    "@context": "${applicationProperties.contexts.core}"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Time`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf(
                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
                    NGSILD_PROPERTY_VALUE to mapOf(
                        "@type" to NGSILD_TIME_TYPE,
                        "@value" to LocalTime.of(11, 20, 30)
                    )
                ),
                "@id" to "urn:ngsi-ld:Beehive:4567",
                "@type" to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "testedAt":{
                        "type": "Property",
                        "value":{
                            "type": "Time",
                            "@value":"11:20:30"
                        }
                    },
                    "@context": "${applicationProperties.contexts.core}"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute property having one instance`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/name" to
                    mapOf(
                        JSONLD_TYPE_KW to "https://uri.etsi.org/ngsi-ld/Property",
                        NGSILD_PROPERTY_VALUE to "ruche",
                        NGSILD_DATASET_ID_IRI to mapOf(
                            JSONLD_ID_KW to "urn:ngsi-ld:Property:french-name"
                        )
                    ),
                JSONLD_ID_KW to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE_KW to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "id":"urn:ngsi-ld:Beehive:4567",
                    "type": "Beehive",
                    "name":{"type": "Property","datasetId":"urn:ngsi-ld:Property:french-name","value":"ruche"},
                    "@context": "${applicationProperties.contexts.core}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute property having more than one instance`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/name" to
                    listOf(
                        mapOf(
                            JSONLD_TYPE_KW to "https://uri.etsi.org/ngsi-ld/Property",
                            NGSILD_PROPERTY_VALUE to "beehive",
                            NGSILD_DATASET_ID_IRI to mapOf(
                                JSONLD_ID_KW to "urn:ngsi-ld:Property:english-name"
                            )
                        ),
                        mapOf(
                            JSONLD_TYPE_KW to "https://uri.etsi.org/ngsi-ld/Property",
                            NGSILD_PROPERTY_VALUE to "ruche",
                            NGSILD_DATASET_ID_IRI to mapOf(
                                JSONLD_ID_KW to "urn:ngsi-ld:Property:french-name"
                            )
                        )
                    ),
                JSONLD_ID_KW to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE_KW to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                 {
                    "id":"urn:ngsi-ld:Beehive:4567",
                    "type": "Beehive",
                    "name":[
                        {
                            "type": "Property","datasetId":"urn:ngsi-ld:Property:english-name","value":"beehive"
                        },
                        {
                            "type": "Property","datasetId":"urn:ngsi-ld:Property:french-name","value":"ruche"
                        }
                    ],
                    "@context": "${applicationProperties.contexts.core}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute relationship having one instance`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
                    mapOf(
                        JSONLD_TYPE_KW to "https://uri.etsi.org/ngsi-ld/Relationship",
                        NGSILD_RELATIONSHIP_OBJECT to mapOf(
                            JSONLD_ID_KW to "urn:ngsi-ld:Beekeeper:1230"
                        ),
                        NGSILD_DATASET_ID_IRI to mapOf(
                            JSONLD_ID_KW to "urn:ngsi-ld:Dataset:managedBy:0215"
                        )
                    ),
                JSONLD_ID_KW to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE_KW to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "id":"urn:ngsi-ld:Beehive:4567",
                    "type": "Beehive",
                    "managedBy": {
                      "type": "Relationship",
                       "datasetId":"urn:ngsi-ld:Dataset:managedBy:0215",
                        "object":"urn:ngsi-ld:Beekeeper:1230"
                    },
                    "@context": "${applicationProperties.contexts.core}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should include createdAt & modifiedAt if query param sysAttrs is present`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
                    mapOf(
                        JSONLD_TYPE_KW to "https://uri.etsi.org/ngsi-ld/Relationship",
                        NGSILD_RELATIONSHIP_OBJECT to mapOf(
                            JSONLD_ID_KW to "urn:ngsi-ld:Beekeeper:1230"
                        ),
                        NGSILD_DATASET_ID_IRI to mapOf(
                            JSONLD_ID_KW to "urn:ngsi-ld:Dataset:managedBy:0215"
                        ),
                        NGSILD_CREATED_AT_IRI to
                            mapOf(
                                "@type" to NGSILD_DATE_TIME_TYPE,
                                "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                            ),
                        NGSILD_MODIFIED_AT_IRI to
                            mapOf(
                                "@type" to NGSILD_DATE_TIME_TYPE,
                                "@value" to Instant.parse("2015-10-18T12:20:30.000001Z").atZone(ZoneOffset.UTC)
                            )
                    ),
                JSONLD_ID_KW to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE_KW to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").isEqualTo("2015-10-18T11:20:30.000001Z")
            .jsonPath("$..modifiedAt").isEqualTo("2015-10-18T12:20:30.000001Z")
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute relationship having more than one instance`() {
        initializeRetrieveEntityMocks()
        coEvery { entityQueryService.queryEntity(any()) } returns ExpandedEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
                    listOf(
                        mapOf(
                            JSONLD_TYPE_KW to "https://uri.etsi.org/ngsi-ld/Relationship",
                            NGSILD_RELATIONSHIP_OBJECT to mapOf(
                                JSONLD_ID_KW to "urn:ngsi-ld:Beekeeper:1229"
                            )
                        ),
                        mapOf(
                            JSONLD_TYPE_KW to "https://uri.etsi.org/ngsi-ld/Relationship",
                            NGSILD_RELATIONSHIP_OBJECT to mapOf(
                                JSONLD_ID_KW to "urn:ngsi-ld:Beekeeper:1230"
                            ),
                            NGSILD_DATASET_ID_IRI to mapOf(
                                JSONLD_ID_KW to "urn:ngsi-ld:Dataset:managedBy:0215"
                            )
                        )
                    ),
                JSONLD_ID_KW to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE_KW to listOf("Beehive")
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                 {
                    "id":"urn:ngsi-ld:Beehive:4567",
                    "type": "Beehive",
                    "managedBy":[
                       {
                          "type": "Relationship",
                          "object":"urn:ngsi-ld:Beekeeper:1229"
                       },
                       {
                          "type": "Relationship",
                          "datasetId":"urn:ngsi-ld:Dataset:managedBy:0215",
                          "object":"urn:ngsi-ld:Beekeeper:1230"
                       }
                    ],
                    "@context": "${applicationProperties.contexts.core}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should return 404 when entity does not exist`() {
        initializeRetrieveEntityMocks()
        coEvery {
            entityQueryService.queryEntity(any())
        } returns ResourceNotFoundException(entityNotFoundMessage("urn:ngsi-ld:BeeHive:TEST")).left()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title": "${entityNotFoundMessage("urn:ngsi-ld:BeeHive:TEST")}"
                    }
                """
            )
    }

    @Test
    fun `get entity by id should return 403 if user is not authorized to read an entity`() {
        initializeRetrieveEntityMocks()
        coEvery {
            entityQueryService.queryEntity("urn:ngsi-ld:BeeHive:TEST".toUri())
        } returns AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:BeeHive:TEST").left()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden read access to entity urn:ngsi-ld:BeeHive:TEST"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should return the warnings sent by the CSRs`() {
        val csr = gimmeRawCSR()
        coEvery {
            entityQueryService.queryEntity("urn:ngsi-ld:BeeHive:TEST".toUri())
        } returns ResourceNotFoundException("no entity").left()

        coEvery {
            distributedEntityConsumptionService
                .distributeRetrieveEntityOperation(any(), any(), any(), any())
        } returns (
            listOf(
                MiscellaneousWarning("message with\nline\nbreaks", csr),
                MiscellaneousWarning("message", csr)
            ) to emptyList()
            )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .exchange()
            .expectStatus().isNotFound
            .expectHeader().valueEquals(
                NGSILDWarning.HEADER_NAME,
                "199 urn:ngsi-ld:ContextSourceRegistration:test \"message with line breaks\"",
                "199 urn:ngsi-ld:ContextSourceRegistration:test \"message\""
            )
    }

    fun initializeQueryEntitiesMocks() {
        val compactedEntities = slot<List<CompactedEntity>>()

        coEvery {
            linkedEntityService.processLinkedEntities(capture(compactedEntities), any())
        } answers {
            compactedEntities.captured.right()
        }
    }

    @Test
    fun `get entities by type should not include temporal properties if query param sysAttrs is not present`() {
        initializeQueryEntitiesMocks()
        coEvery { entityQueryService.queryEntities(any()) } returns Pair(
            listOf(
                ExpandedEntity(
                    mapOf(
                        "@id" to beehiveId.toString(),
                        "@type" to listOf("Beehive")
                    )
                )
            ),
            1
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities?type=Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                [
                    {
                        "id": "$beehiveId",
                        "type": "Beehive",
                        "@context": "${applicationProperties.contexts.core}"
                    }
                ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get entities by type should include temporal properties if optional query param sysAttrs is present`() {
        initializeQueryEntitiesMocks()
        coEvery {
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = "https://uri.etsi.org/ngsi-ld/default-context/Beehive",
                    paginationQuery = PaginationQuery(offset = 0, limit = 30),
                    contexts = listOf(applicationProperties.contexts.core)
                )
            )
        } returns Pair(
            listOf(
                ExpandedEntity(
                    mapOf(
                        NGSILD_CREATED_AT_IRI to
                            mapOf(
                                "@type" to NGSILD_DATE_TIME_TYPE,
                                "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                            ),
                        "@id" to beehiveId.toString(),
                        "@type" to listOf("Beehive")
                    )
                )
            ),
            1
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities?type=Beehive&options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                [
                    {
                        "id": "$beehiveId",
                        "type": "Beehive",
                        "createdAt":"2015-10-18T11:20:30.000001Z",
                        "@context": "${applicationProperties.contexts.core}"
                    }
                ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entities should return 200 with prev and next link header if exists`() {
        initializeQueryEntitiesMocks()
        coEvery { entityQueryService.queryEntities(any()) } returns Pair(
            listOf(
                ExpandedEntity(
                    mapOf("@id" to "urn:ngsi-ld:Beehive:TESTC", "@type" to listOf("Beehive"))
                )
            ),
            3
        ).right()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/entities/?type=Beehive" +
                    "&id=urn:ngsi-ld:Beehive:TESTC,urn:ngsi-ld:Beehive:TESTB,urn:ngsi-ld:Beehive:TESTD&limit=1&offset=1"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(
                "Link",
                """
                    </ngsi-ld/v1/entities?type=Beehive&id=urn:ngsi-ld:Beehive:TESTC,urn:ngsi-ld:Beehive:TESTB,urn:ngsi-ld:Beehive:TESTD&limit=1&offset=0>;rel="prev";type="application/ld+json"
                """.trimIndent(),
                """
                    </ngsi-ld/v1/entities?type=Beehive&id=urn:ngsi-ld:Beehive:TESTC,urn:ngsi-ld:Beehive:TESTB,urn:ngsi-ld:Beehive:TESTD&limit=1&offset=2>;rel="next";type="application/ld+json"
                """.trimIndent()
            )
            .expectBody().json(
                """
                [
                    {
                        "id": "urn:ngsi-ld:Beehive:TESTC",
                        "type": "Beehive",
                        "@context": "${applicationProperties.contexts.core}"
                    }
                ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entities should return 200 and empty response if requested offset does not exists`() {
        initializeQueryEntitiesMocks()
        coEvery {
            entityQueryService.queryEntities(any())
        } returns Pair(emptyList<ExpandedEntity>(), 0).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/?type=Beehive&limit=1&offset=9")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `get entities should return 400 if limit is equal or less than zero`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities/?type=Beehive&limit=-1&offset=1")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Offset must be greater than zero and limit must be strictly greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities should return 403 if limit is greater than the maximum authorized limit`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities/?type=Beehive&limit=200&offset=1")
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/TooManyResults",
                    "title": "You asked for 200 results, but the supported maximum limit is 100"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities with id and type should return 200`() {
        initializeQueryEntitiesMocks()
        coEvery {
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    ids = setOf(beehiveId),
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(offset = 0, limit = 30),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            )
        } returns Pair(
            listOf(
                ExpandedEntity(
                    mapOf(
                        "@id" to beehiveId.toString(),
                        "@type" to listOf("Beehive")
                    )
                )
            ),
            1
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities?id=$beehiveId&type=$BEEHIVE_TERM")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                [
                    {
                        "id": "$beehiveId",
                        "type": "Beehive",
                        "@context": "$APIC_COMPOUND_CONTEXT"
                    }
                ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entities should return 200 and the number of results`() {
        initializeQueryEntitiesMocks()
        coEvery {
            entityQueryService.queryEntities(any())
        } returns Pair(emptyList<ExpandedEntity>(), 3).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/?type=Beehive&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get entities should return 400 if the number of results is requested with a limit less than zero`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities/?type=Beehive&limit=-1&offset=1&count=true")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Offset and limit must be greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities should allow a query not including a type request parameter`() {
        initializeQueryEntitiesMocks()
        coEvery {
            entityQueryService.queryEntities(any())
        } returns Pair(emptyList<ExpandedEntity>(), 0).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/?attrs=myProp")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `get entities should return 400 if required parameters are missing`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query unless local is true"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities should return the warnings sent by the CSRs`() {
        val csr = gimmeRawCSR()
        initializeQueryEntitiesMocks()

        coEvery {
            entityQueryService.queryEntities(any())
        } returns (emptyList<ExpandedEntity>() to 0).right()

        coEvery {
            distributedEntityConsumptionService
                .distributeQueryEntitiesOperation(any(), any(), any())
        } returns Triple(
            listOf(
                MiscellaneousWarning("message with\nline\nbreaks", csr),
                MiscellaneousWarning("message", csr)
            ),
            emptyList(), emptyList()
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities?type=$BEEHIVE_TERM&count=true")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(
                NGSILDWarning.HEADER_NAME,
                "199 urn:ngsi-ld:ContextSourceRegistration:test \"message with line breaks\"",
                "199 urn:ngsi-ld:ContextSourceRegistration:test \"message\""
            ).expectHeader().valueEquals(RESULTS_COUNT_HEADER, "0")
    }

    @Test
    fun `replace entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.replaceEntity(any(), any<NgsiLdEntity>(), any())
        } returns EMPTY_UPDATE_RESULT.right()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.replaceEntity(
                eq(breedingServiceId),
                match<NgsiLdEntity> {
                    it.id == breedingServiceId
                },
                any()
            )
        }
    }

    @Test
    fun `replace entity should return a 403 if user is not allowed to update the entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.replaceEntity(breedingServiceId, any(), any())
        } returns AccessDeniedException("User forbidden to modify entity").left()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden to modify entity"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `replace entity should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.replaceEntity(breedingServiceId, any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(breedingServiceId.toString())).left()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `replace entity should return a 400 if id contained in payload is different from the one in URL`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0215".toUri()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "The id contained in the body is not the same as the one provided in the URL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `replace entity should return a 400 if entity id is not present in the path`() {
        webClient.put()
            .uri("/ngsi-ld/v1/entities/")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Missing entity id when trying to replace an entity"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `replace entity should return the result from the BatchApiResponse`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val capturedExpandedEntity = slot<ExpandedEntity>()
        val result = mockkClass(BatchOperationResult::class, relaxed = true)
        val response = "test response"

        coEvery {
            distributedEntityProvisionService
                .distributeReplaceEntity(capture(capturedExpandedEntity), any(), any())
        } answers { result to capturedExpandedEntity.captured }

        coEvery {
            entityService.replaceEntity(breedingServiceId, any(), any())
        } returns EMPTY_UPDATE_RESULT.right()

        every { result.addEither(any(), any(), any()) } returns Unit

        every {
            result.toNonBatchEndpointResponse(any(), any())
        } returns ResponseEntity.status(HttpStatus.MULTI_STATUS).body(response)

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(207)
            .expectBody().equals(response)

        coVerify { result.addEither(any(), any()) }
        coVerify { result.toNonBatchEndpointResponse(any(), any()) }
    }

    @Test
    fun `replace entity should not try to call the context sources if local is true`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.replaceEntity(breedingServiceId, any(), any())
        } returns EMPTY_UPDATE_RESULT.right()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId?local=true")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify(exactly = 0) { distributedEntityProvisionService.distributeReplaceEntity(any(), any(), any()) }
    }

    @Test
    fun `append entity attribute should return a 204 if all attributes were appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendResult = UpdateResult(
            listOf(fishNumberAttribute),
            emptyList()
        )

        coEvery {
            entityService.appendAttributes(any(), any(), any())
        } returns appendResult.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.appendAttributes(
                eq(entityId),
                any(),
                eq(false)
            )
        }
    }

    @Test
    fun `append entity attribute should return a 207 if some attributes could not be appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_twoNewProperties.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendResult = UpdateResult(
            listOf(fishNumberAttribute),
            listOf(NotUpdatedDetails(fishSizeAttribute, "overwrite disallowed"))
        )

        coEvery {
            entityService.appendAttributes(any(), any(), any())
        } returns appendResult.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "updated":["$fishNumberAttribute"],
                    "notUpdated":[{"attributeName":"$fishSizeAttribute","reason":"overwrite disallowed"}]
                } 
                """.trimIndent()
            )

        coVerify {
            entityService.appendAttributes(
                eq(entityId),
                any(),
                eq(false)
            )
        }
    }

    @Test
    fun `append entity attribute should return a 204 when adding a new type to an entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newType.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendTypeResult = UpdateResult(
            listOf(JSONLD_TYPE_KW),
            emptyList()
        )

        coEvery {
            entityService.appendAttributes(any(), any(), any())
        } returns appendTypeResult.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.NO_CONTENT)

        coVerify {
            entityService.appendAttributes(
                eq(entityId),
                any(),
                eq(false)
            )
        }
    }

    @Test
    fun `append entity attribute should return a 207 if types or attributes could not be appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newInvalidTypeAndAttribute.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.appendAttributes(any(), any(), any())
        } returns UpdateResult(
            updated = listOf(fishNumberAttribute),
            notUpdated = listOf(
                NotUpdatedDetails(JSONLD_TYPE_KW, "Append operation has unexpectedly failed"),
                NotUpdatedDetails(fishSizeAttribute, "overwrite disallowed")
            )
        ).right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "updated":["$fishNumberAttribute"],
                    "notUpdated":[
                      {"attributeName":"$fishSizeAttribute","reason":"overwrite disallowed"},
                      {"attributeName":"$JSONLD_TYPE_KW","reason":"Append operation has unexpectedly failed"}
                    ]
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `append entity attribute should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.appendAttributes(eq(entityId), any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title": "Entity urn:ngsi-ld:BreedingService:0214 was not found"
                    }
                """.trimIndent()
            )
    }

    @Test
    @SuppressWarnings("MaxLineLength")
    fun `append entity attribute should return a 400 if the attribute is not NGSI-LD valid`() {
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val invalidPayload =
            """
            {
                "connectsTo":{
                    "type": "Relationship"
                }
            }
            """.trimIndent()

        coEvery {
            entityService.appendAttributes(any(), any(), any())
        } returns BadRequestDataException(
            "Relationship https://ontology.eglobalmark.com/egm#connectsTo does not have an object field"
        ).left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(invalidPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Relationship https://ontology.eglobalmark.com/egm#connectsTo does not have an object field"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `append entity attribute should return a 403 if user is not allowed to update entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.appendAttributes(entityId, any(), any())
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:BreedingService:0214").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                { 
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied", 
                    "title": "User forbidden write access to entity urn:ngsi-ld:BreedingService:0214", 
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `partial attribute update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = loadSampleData("aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"
        val updateResult = UpdateResult(
            updated = listOf(fishNumberAttribute),
            notUpdated = emptyList()
        )

        coEvery {
            entityService.partialUpdateAttribute(any(), any())
        } returns updateResult.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.partialUpdateAttribute(eq(entityId), any())
        }
    }

    @Test
    fun `partial attribute update should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        coEvery {
            entityService.partialUpdateAttribute(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `partial attribute update should return a 404 if attribute does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        coEvery {
            entityService.partialUpdateAttribute(any(), any())
        } returns UpdateResult(
            updated = arrayListOf(),
            notUpdated = arrayListOf(
                NotUpdatedDetails(
                    fishNumberAttribute,
                    "Unknown attribute $fishNumberAttribute with datasetId urn:ngsi-ld:Dataset:1 in entity $entityId"
                )
            )
        ).right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        coVerify {
            entityService.partialUpdateAttribute(eq(entityId), any())
        }
    }

    @Test
    fun `partial attribute update should not authorize user without write rights on entity to update attribute`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        coEvery {
            entityService.partialUpdateAttribute(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                { 
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied", 
                    "title": "User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN", 
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updateResult = UpdateResult(
            updated = listOf(fishNumberAttribute, fishSizeAttribute),
            notUpdated = emptyList()
        )

        coEvery {
            entityService.mergeEntity(any(), any(), any())
        } returns updateResult.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.mergeEntity(eq(entityId), any(), any())
        }
    }

    @Test
    fun `merge entity should return a 204 if JSON-LD payload is correct and use observedAt parameter`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updateResult = UpdateResult(
            updated = listOf(fishNumberAttribute, fishSizeAttribute),
            notUpdated = emptyList()
        )

        coEvery {
            entityService.mergeEntity(any(), any(), any())
        } returns updateResult.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId?observedAt=2019-12-04T12:00:00.00Z")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.mergeEntity(
                eq(entityId),
                any(),
                eq(ZonedDateTime.parse("2019-12-04T12:00:00.00Z"))
            )
        }
    }

    @Test
    fun `merge entity should return a 400 if optional parameter observedAt is not a datetime`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId?observedAt=notDateTime")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "'observedAt' parameter is not a valid date"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery {
            entityService.mergeEntity(any(), any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                  "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                  "title": "${entityNotFoundMessage(entityId.toString())}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 403 if user is not allowed to update it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery {
            entityService.mergeEntity(any(), any(), any())
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 400 if entityId is missing`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Missing entity id when trying to merge an entity"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `entity attributes update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updateResult = UpdateResult(
            updated = listOf(fishNumberAttribute),
            notUpdated = emptyList()
        )

        coEvery {
            entityService.updateAttributes(any(), any())
        } returns updateResult.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.updateAttributes(eq(entityId), any())
        }
    }

    @Test
    fun `entity attributes update should return a 207 if some relationships objects are not found`() {
        val jsonLdFile = ClassPathResource(
            "/ngsild/aquac/fragments/DeadFishes_updateEntityAttributes_invalidAttribute.json"
        )
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val notUpdatedAttribute = NotUpdatedDetails("removedFrom", "Property is not valid")

        coEvery {
            entityService.updateAttributes(any(), any())
        } returns UpdateResult(
            updated = listOf(fishNumberAttribute),
            notUpdated = listOf(notUpdatedAttribute)
        ).right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
    }

    @Test
    fun `entity attributes update should return a 207 if types could not be updated`() {
        val jsonLdFile = ClassPathResource(
            "/ngsild/aquac/fragments/DeadFishes_updateEntityAttributes_invalidType.json"
        )
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery {
            entityService.updateAttributes(any(), any())
        } returns UpdateResult(
            updated = emptyList(),
            notUpdated = listOf(NotUpdatedDetails("type", "A type cannot be removed"))
        ).right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)

        coVerify {
            entityService.updateAttributes(eq(entityId), any())
        }
    }

    @Test
    @SuppressWarnings("MaxLineLength")
    fun `entity attributes update should return a 503 if JSON-LD context is not correct`() {
        val payload =
            """
            {
                "name" : "My precious sensor Updated",
                "trigger" : "on"
            }
            """.trimIndent()
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()
        val wrongContext = buildContextLinkHeader("https://easyglobalmarket.com/contexts/diat.jsonld")

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", wrongContext)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(payload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable",
                    "title": "Unable to load remote context",
                    "detail": "caused by: JsonLdError[code=There was a problem encountered loading a remote context [code=LOADING_REMOTE_CONTEXT_FAILED]., message=There was a problem encountered loading a remote context [https://easyglobalmarket.com/contexts/diat.jsonld]]"
                }
                """.trimIndent()
            )

        verify { entityService wasNot called }
    }

    @Test
    fun `entity attributes update should return a 404 if entity does not exist`() {
        val payload =
            """
            {
                "temperature": { "type": "Property", "value" : 17.0 },
                "@context": ["$APIC_COMPOUND_CONTEXT"]
            }
            """.trimIndent()

        coEvery {
            entityService.updateAttributes(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs")
            .bodyValue(payload)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title": "Entity $beehiveId was not found"
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `entity attributes update should return a 403 if user is not allowed to update it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()

        coEvery {
            entityService.updateAttributes(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:Sensor:0022CCC").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden write access to entity urn:ngsi-ld:Sensor:0022CCC"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity should return a 204 if an entity has been successfully deleted`() {
        coEvery { entityService.deleteEntity(any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteEntity(eq(beehiveId))
        }
    }

    @Test
    fun `delete entity should return a 404 if entity to be deleted has not been found`() {
        coEvery {
            entityService.deleteEntity(beehiveId)
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                  "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                  "title": "${entityNotFoundMessage(beehiveId.toString())}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity should return a 500 if entity could not be deleted`() {
        coEvery {
            entityService.deleteEntity(any())
        } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/InternalError",
                      "title": "java.lang.RuntimeException: Unexpected server error"
                    }
                    """
            )
    }

    @Test
    fun `delete entity should return a 403 is user is not authorized to delete an entity`() {
        coEvery {
            entityService.deleteEntity(beehiveId)
        } returns AccessDeniedException("User forbidden admin access to entity $beehiveId").left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden admin access to entity $beehiveId"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 204 if the attribute has been successfully deleted`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_IRI),
                null,
                eq(false)
            )
        }
    }

    @Test
    fun `delete entity attribute should delete all instances if deleteAll flag is true`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_TERM?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_IRI),
                null,
                eq(true)
            )
        }
    }

    @Test
    fun `delete entity attribute should delete instance with the provided datasetId`() {
        val datasetId = "urn:ngsi-ld:Dataset:temperature:1"
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_TERM?datasetId=$datasetId")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_IRI),
                eq(datasetId.toUri()),
                eq(false)
            )
        }
    }

    @Test
    fun `delete entity attribute should return a 404 if the entity is not found`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                      {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title": "Entity urn:ngsi-ld:BeeHive:TESTC was not found"
                      }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 404 if the attribute is not found`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any())
        } throws ResourceNotFoundException("Attribute Not Found")

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_TERM?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                      {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title": "Attribute Not Found"
                      }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 400 if the request is not correct`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any())
        } returns BadRequestDataException("Something is wrong with the request").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST)
            .expectBody().json(
                """
                {
                  "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                  "title": "Something is wrong with the request"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 403 if user is not allowed to update entity`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any())
        } returns AccessDeniedException("User forbidden write access to entity $beehiveId").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden write access to entity urn:ngsi-ld:BeeHive:TESTC"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity should return a 207 if some contextSources failed`() {
        val error = BatchEntityError(
            entityId = "urn:ngsi-ld:BreedingService:0214".toUri(),
            error = ConflictException("error").toProblemDetail(),
            registrationId = "id:2".toUri()
        )
        val firstResult = BatchOperationResult(
            mutableListOf(BatchEntitySuccess("id:1".toUri())),
            mutableListOf(error)
        )
        coEvery {
            distributedEntityProvisionService.distributeDeleteEntity(any(), any())
        } returns firstResult

        coEvery {
            entityService.deleteEntity(any())
        } returns AccessDeniedException("User forbidden to delete entities").left()

        val expectedJson = """
                {
                    "success": ["id:1"],
                    "errors":[
                        {
                            "entityId":"urn:ngsi-ld:BreedingService:0214",
                            "error":{
                                "type": "https://uri.etsi.org/ngsi-ld/errors/Conflict",
                                "title": "error"
                            },
                            "registrationId": "id:2"
                        },
                        {
                            "entityId":"urn:ngsi-ld:BreedingService:0214",
                            "error":{
                                "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                                "title": "User forbidden to delete entities"
                            },
                            "registrationId":null
                        }
                    ]
                }
        """.trimIndent()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BreedingService:0214")
            .exchange()
            .expectStatus().isEqualTo(207)
            .expectBody().json(expectedJson)
    }

    @Test
    fun `replace attribute should return a 204 if attribute has been successfully replaced`() {
        val attributeFragment = ClassPathResource("/ngsild/fragments/beehive_new_incoming_property.json")
        coEvery {
            entityService.replaceAttribute(any(), any())
        } returns UpdateResult(
            updated = listOf(INCOMING_IRI),
            notUpdated = emptyList()
        ).right()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$INCOMING_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(attributeFragment)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.replaceAttribute(
                beehiveId,
                match {
                    it.first == INCOMING_IRI
                }
            )
        }
    }

    @Test
    fun `replace attribute should return a 404 if the attribute does not exist`() {
        val attributeFragment = ClassPathResource("/ngsild/fragments/beehive_new_incoming_property.json")
        coEvery {
            entityService.replaceAttribute(any(), any())
        } returns UpdateResult(
            updated = emptyList(),
            notUpdated = arrayListOf(
                NotUpdatedDetails(
                    INCOMING_IRI,
                    "Unknown attribute $INCOMING_IRI in entity $beehiveId"
                )
            )
        ).right()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$INCOMING_TERM")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(attributeFragment)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title": "Unknown attribute $INCOMING_IRI in entity $beehiveId"
                    }
                    """
            )
    }
}
