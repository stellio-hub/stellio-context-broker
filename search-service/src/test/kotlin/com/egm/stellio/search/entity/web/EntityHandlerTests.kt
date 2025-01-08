package com.egm.stellio.search.entity.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
import com.egm.stellio.search.entity.compaction.EntityCompactionService
import com.egm.stellio.search.entity.model.NotUpdatedDetails
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.model.UpdatedDetails
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.DEFAULT_DETAIL
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.AQUAC_HEADER_LINK
import com.egm.stellio.shared.util.BEEHIVE_COMPACT_TYPE
import com.egm.stellio.shared.util.INCOMING_COMPACT_PROPERTY
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TERM
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.TEMPERATURE_COMPACT_PROPERTY
import com.egm.stellio.shared.util.TEMPERATURE_PROPERTY
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.sub
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.called
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.verify
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
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.lang.reflect.UndeclaredThrowableException
import java.time.Instant
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
    private lateinit var entitySourceService: EntityCompactionService

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

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
    fun mockCSR() {
        coEvery {
            contextSourceRegistrationService
                .getContextSourceRegistrations(any(), any(), any())
        } returns listOf()
    }

    private val beehiveId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
    private val fishSizeAttribute = "https://ontology.eglobalmark.com/aquac#fishSize"

    @Test
    fun `create entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.createEntity(any<NgsiLdEntity>(), any(), any())
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
                any(),
                any()
            )
        }
    }

    @Test
    fun `create entity should return a 409 if the entity already exists`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        coEvery {
            entityService.createEntity(any(), any(), MOCK_USER_SUB)
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
                      "title": "Already Exists",
                      "detail": "$DEFAULT_DETAIL"
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 500 error if there is an internal server error`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        coEvery {
            entityService.createEntity(any<NgsiLdEntity>(), any(), any())
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
                      "title": "Internal Server Exception",
                      "detail": "$DEFAULT_DETAIL"
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
            entityService.createEntity(any<NgsiLdEntity>(), any(), any())
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
                    "title": "Target entity does not exist",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 403 if user is not allowed to create entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        coEvery {
            entityService.createEntity(any(), any(), sub.getOrNull())
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
                    "title": "User forbidden to create entities",
                    "detail": "$DEFAULT_DETAIL"
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
                    "title": "The ['invalid'] parameters are not allowed on this endpoint. This endpoint does not accept any query parameters. ",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 501 if it contains a not implemented query parameter`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities?local=true")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(501)
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/NotImplemented",
                    "title": "The ['local'] parameters have not been implemented yet. This endpoint does not accept any query parameters. ",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    private fun mockEntitySourceSuccess(entity: CompactedEntity) {
        coEvery { entitySourceService.getEntityFromSources(MOCK_USER_SUB, any(), any(), any(), any(), any()) } returns
            (listOf(entity).right() to emptyList())
    }

    @Test
    fun `get entity by id should return 200 when entity exists`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "fr": "Grand Place",
                        "nl": "Grote Markt",
                        "@none": "Big Place"
                    }
                }
            }
        """.trimIndent()
            .deserializeAsMap()
        mockEntitySourceSuccess(compactedEntity)

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `get entity by id should correctly serialize temporal properties`() {
        mockEntitySourceSuccess(
            mapOf(
                NGSILD_CREATED_AT_TERM to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC),
                "id" to beehiveId.toString(),
                "type" to listOf("Beehive")
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                {
                    "createdAt": "2015-10-18T11:20:30.000001Z",
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly return the simplified representation of an entity`() {
        mockEntitySourceSuccess(
            mapOf(
                "id" to beehiveId.toString(),
                "type" to "Beehive",
                "prop1" to mapOf(
                    JSONLD_TYPE_TERM to NGSILD_PROPERTY_TERM,
                    JSONLD_VALUE_TERM to "some value"
                ),
                "rel1" to mapOf(
                    JSONLD_TYPE_TERM to NGSILD_RELATIONSHIP_TERM,
                    JSONLD_OBJECT to "urn:ngsi-ld:Entity:1234"
                ),
                "@context" to applicationProperties.contexts.core
            )
        )

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
    fun `get entity by id should not include temporal properties if optional query param sysAttrs is not present`() {
        mockEntitySourceSuccess(
            mapOf(
                "id" to beehiveId.toString(),
                "type" to BEEHIVE_COMPACT_TYPE,
                "createdAt" to ngsiLdDateTime(),
                "modifiedAt" to ngsiLdDateTime()
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("""{"type":"$BEEHIVE_COMPACT_TYPE"}""")
            .jsonPath("$.createdAt").doesNotExist()
            .jsonPath("$.modifiedAt").doesNotExist()
    }

    @Test
    fun `get entity by id should include createdAt & modifiedAt if query param sysAttrs is present`() {
        mockEntitySourceSuccess(
            mapOf(
                "managedBy" to
                    mapOf(
                        JSONLD_TYPE_TERM to "Relationship",
                        JSONLD_OBJECT to "urn:ngsi-ld:Beekeeper:1230",
                        NGSILD_DATASET_ID_TERM to mapOf(
                            JSONLD_ID_TERM to "urn:ngsi-ld:Dataset:managedBy:0215"
                        ),
                        NGSILD_CREATED_AT_TERM to
                            Instant.parse("2015-10-18T13:20:30.000001Z").atZone(ZoneOffset.UTC),
                        NGSILD_MODIFIED_AT_TERM to
                            Instant.parse("2015-10-18T14:20:30.000001Z").atZone(ZoneOffset.UTC)
                    ),
                JSONLD_ID_TERM to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE_TERM to "Beehive",
                NGSILD_CREATED_AT_TERM to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC),
                NGSILD_MODIFIED_AT_TERM to Instant.parse("2015-10-18T12:20:30.000001Z").atZone(ZoneOffset.UTC)
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId?options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .json(
                """
                {
                  createdAt: "2015-10-18T11:20:30.000001Z",
                  modifiedAt: "2015-10-18T12:20:30.000001Z",
                  managedBy: {
                    createdAt: "2015-10-18T13:20:30.000001Z",
                    modifiedAt: "2015-10-18T14:20:30.000001Z"
                  }
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should return the received error`() {
        coEvery {
            entitySourceService.getEntityFromSources(MOCK_USER_SUB, any(), any(), any(), any(), any())
        } returns
            (ResourceNotFoundException(entityNotFoundMessage("urn:ngsi-ld:BeeHive:TEST")).left() to emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title": "${entityNotFoundMessage("urn:ngsi-ld:BeeHive:TEST")}",
                      "detail": "$DEFAULT_DETAIL"
                    }
                """
            )
    }

    @Test
    fun `get entity by id should return the warnings for success and errors`() {
        val csr = gimmeRawCSR()
        coEvery {
            entitySourceService.getEntityFromSources(
                MOCK_USER_SUB,
                any(),
                any(),
                any(),
                any(),
                any()
            )
        } returns
            (
                ResourceNotFoundException("no entity").left() to listOf(
                    MiscellaneousWarning(
                        "message with\nline\nbreaks",
                        csr
                    ),
                    MiscellaneousWarning("message", csr)
                )
                ) andThen (
                emptyList<CompactedEntity>().right() to listOf(
                    MiscellaneousWarning(
                        "message with\nline\nbreaks",
                        csr
                    ),
                    MiscellaneousWarning("message", csr)
                )
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

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(
                NGSILDWarning.HEADER_NAME,
                "199 urn:ngsi-ld:ContextSourceRegistration:test \"message with line breaks\"",
                "199 urn:ngsi-ld:ContextSourceRegistration:test \"message\""
            )
    }

    fun mockGetEntitiesFromSourceSuccess(
        entities: List<CompactedEntity>,
        count: Int = 0,
        warnings: List<NGSILDWarning> = emptyList()
    ) {
        coEvery {
            entitySourceService.getEntitiesFromSources(any(), any(), any(), any(), any())
        } returns Triple(entities, count, warnings).right()
    }

    @Test
    fun `get entities by type should not include temporal properties if query param sysAttrs is not present`() {
        mockGetEntitiesFromSourceSuccess(
            listOf(
                mapOf(
                    "id" to beehiveId.toString(),
                    "type" to "Beehive",
                    NGSILD_CREATED_AT_TERM to ngsiLdDateTime(),
                    NGSILD_MODIFIED_AT_TERM to ngsiLdDateTime(),
                    "@context" to applicationProperties.contexts.core
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
    fun `get entities by type should include temporal properties if query param sysAttrs is present`() {
        mockGetEntitiesFromSourceSuccess(
            listOf(
                mapOf(
                    "id" to beehiveId.toString(),
                    "type" to "Beehive",
                    NGSILD_CREATED_AT_TERM to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC),
                    NGSILD_MODIFIED_AT_TERM to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC),
                    "@context" to applicationProperties.contexts.core
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
                        "modifiedAt":"2015-10-18T11:20:30.000001Z",
                        "@context": "${applicationProperties.contexts.core}"
                    }
                ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entities should return 200 with prev and next link header if exists`() {
        mockGetEntitiesFromSourceSuccess(
            listOf(
                mapOf(
                    "id" to "urn:ngsi-ld:Beehive:TESTC",
                    "type" to "Beehive",
                    "@context" to applicationProperties.contexts.core
                )
            ),
            3
        )

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
        mockGetEntitiesFromSourceSuccess(emptyList())

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
                    "title": "Offset must be greater than zero and limit must be strictly greater than zero",
                    "detail": "$DEFAULT_DETAIL"
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
                    "title": "You asked for 200 results, but the supported maximum limit is 100",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities with id and type should return 200`() {
        mockGetEntitiesFromSourceSuccess(
            listOf(
                mapOf(
                    "id" to beehiveId.toString(),
                    "type" to "Beehive",
                    "@context" to APIC_COMPOUND_CONTEXT
                )
            ),
            1
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities?id=$beehiveId&type=$BEEHIVE_COMPACT_TYPE")
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
        mockGetEntitiesFromSourceSuccess(emptyList(), 3)
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
                    "title": "Offset and limit must be greater than zero",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities should allow a query not including a type request parameter`() {
        mockGetEntitiesFromSourceSuccess(emptyList())

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
                    "title": "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities should return the received warnings`() {
        val csr = gimmeRawCSR()

        mockGetEntitiesFromSourceSuccess(
            emptyList(),
            warnings = listOf(
                MiscellaneousWarning(
                    "message with\nline\nbreaks",
                    csr
                ),
                MiscellaneousWarning("message", csr)
            )
        )

        coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit
        webClient.get()
            .uri("/ngsi-ld/v1/entities?type=$BEEHIVE_COMPACT_TYPE&count=true")
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
            entityService.replaceEntity(any(), any<NgsiLdEntity>(), any(), any())
        } returns Unit.right()

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
                any(),
                any()
            )
        }
    }

    @Test
    fun `replace entity should return a 403 if user is not allowed to update the entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.replaceEntity(breedingServiceId, any(), any(), sub.getOrNull())
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
                    "title": "User forbidden to modify entity",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `replace entity should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/breedingService.jsonld")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.replaceEntity(breedingServiceId, any(), any(), sub.getOrNull())
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
                    "title": "The id contained in the body is not the same as the one provided in the URL",
                    "detail": "$DEFAULT_DETAIL"
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
                    "title": "Missing entity id when trying to replace an entity",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
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
            entityService.appendAttributes(any(), any(), any(), any())
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
                eq(false),
                sub.getOrNull()
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
            entityService.appendAttributes(any(), any(), any(), any())
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
                eq(false),
                sub.getOrNull()
            )
        }
    }

    @Test
    fun `append entity attribute should return a 204 when adding a new type to an entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newType.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendTypeResult = UpdateResult(
            listOf(JSONLD_TYPE),
            emptyList()
        )

        coEvery {
            entityService.appendAttributes(any(), any(), any(), any())
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
                eq(false),
                any()
            )
        }
    }

    @Test
    fun `append entity attribute should return a 207 if types or attributes could not be appended`() {
        val jsonLdFile =
            ClassPathResource("/ngsild/aquac/fragments/BreedingService_newInvalidTypeAndAttribute.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.appendAttributes(any(), any(), any(), any())
        } returns UpdateResult(
            updated = listOf(fishNumberAttribute),
            notUpdated = listOf(
                NotUpdatedDetails(JSONLD_TYPE, "Append operation has unexpectedly failed"),
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
                      {"attributeName":"$JSONLD_TYPE","reason":"Append operation has unexpectedly failed"}
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
            entityService.appendAttributes(eq(entityId), any(), any(), MOCK_USER_SUB)
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
                      "title": "Entity urn:ngsi-ld:BreedingService:0214 was not found",
                      "detail": "$DEFAULT_DETAIL"
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
            entityService.appendAttributes(any(), any(), any(), any())
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
                    "title": "Relationship https://ontology.eglobalmark.com/egm#connectsTo does not have an object field",
                    "detail": "$DEFAULT_DETAIL"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `append entity attribute should return a 403 if user is not allowed to update entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityService.appendAttributes(entityId, any(), any(), sub.getOrNull())
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
                    "detail": "$DEFAULT_DETAIL" 
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
            entityService.partialUpdateAttribute(any(), any(), any())
        } returns updateResult.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.partialUpdateAttribute(eq(entityId), any(), sub.getOrNull())
        }
    }

    @Test
    fun `partial attribute update should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        coEvery {
            entityService.partialUpdateAttribute(any(), any(), MOCK_USER_SUB)
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
            entityService.partialUpdateAttribute(any(), any(), any())
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
            entityService.partialUpdateAttribute(eq(entityId), any(), sub.getOrNull())
        }
    }

    @Test
    fun `partial attribute update should not authorize user without write rights on entity to update attribute`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        coEvery {
            entityService.partialUpdateAttribute(any(), any(), sub.getOrNull())
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
                    "detail": "$DEFAULT_DETAIL" 
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
            entityService.mergeEntity(any(), any(), any(), any())
        } returns updateResult.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.mergeEntity(eq(entityId), any(), any(), any())
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
            entityService.mergeEntity(any(), any(), any(), any())
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
                eq(ZonedDateTime.parse("2019-12-04T12:00:00.00Z")),
                any()
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
                    "title": "'observedAt' parameter is not a valid date",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery {
            entityService.mergeEntity(any(), any(), any(), MOCK_USER_SUB)
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
                  "title": "${entityNotFoundMessage(entityId.toString())}",
                  "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 403 if user is not allowed to update it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery {
            entityService.mergeEntity(any(), any(), any(), MOCK_USER_SUB)
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
                    "title": "User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN",
                    "detail": "$DEFAULT_DETAIL"
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
                    "title": "Missing entity id when trying to merge an entity",
                    "detail": "$DEFAULT_DETAIL"
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
            entityService.updateAttributes(any(), any(), any())
        } returns updateResult.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.updateAttributes(eq(entityId), any(), any())
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
            entityService.updateAttributes(any(), any(), any())
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
            entityService.updateAttributes(any(), any(), any())
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
            entityService.updateAttributes(eq(entityId), any(), any())
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
                    "title": "Unable to load remote context (cause was: JsonLdError[code=There was a problem encountered loading a remote context [code=LOADING_REMOTE_CONTEXT_FAILED]., message=There was a problem encountered loading a remote context [https://easyglobalmarket.com/contexts/diat.jsonld]])",
                    "detail": "$DEFAULT_DETAIL"
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
            entityService.updateAttributes(any(), any(), MOCK_USER_SUB)
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
                      "title": "Entity $beehiveId was not found",
                      "detail": "$DEFAULT_DETAIL"
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `entity attributes update should return a 403 if user is not allowed to update it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()

        coEvery {
            entityService.updateAttributes(any(), any(), MOCK_USER_SUB)
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
                    "title": "User forbidden write access to entity urn:ngsi-ld:Sensor:0022CCC",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity should return a 204 if an entity has been successfully deleted`() {
        coEvery { entityService.deleteEntity(any(), MOCK_USER_SUB) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteEntity(eq(beehiveId), eq(MOCK_USER_SUB))
        }
    }

    @Test
    fun `delete entity should return a 404 if entity to be deleted has not been found`() {
        coEvery {
            entityService.deleteEntity(beehiveId, MOCK_USER_SUB)
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                  "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                  "title": "${entityNotFoundMessage(beehiveId.toString())}",
                  "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity should return a 500 if entity could not be deleted`() {
        coEvery {
            entityService.deleteEntity(any(), MOCK_USER_SUB)
        } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/InternalError",
                      "title": "java.lang.RuntimeException: Unexpected server error",
                      "detail": "$DEFAULT_DETAIL"
                    }
                    """
            )
    }

    @Test
    fun `delete entity should return a 403 is user is not authorized to delete an entity`() {
        coEvery {
            entityService.deleteEntity(beehiveId, sub.getOrNull())
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
                    "title": "User forbidden admin access to entity $beehiveId",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 204 if the attribute has been successfully deleted`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any(), MOCK_USER_SUB)
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                null,
                eq(false),
                eq(MOCK_USER_SUB)
            )
        }
    }

    @Test
    fun `delete entity attribute should delete all instances if deleteAll flag is true`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any(), MOCK_USER_SUB)
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                null,
                eq(true),
                eq(MOCK_USER_SUB)
            )
        }
    }

    @Test
    fun `delete entity attribute should delete instance with the provided datasetId`() {
        val datasetId = "urn:ngsi-ld:Dataset:temperature:1"
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any(), MOCK_USER_SUB)
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY?datasetId=$datasetId")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                eq(datasetId.toUri()),
                eq(false),
                eq(MOCK_USER_SUB)
            )
        }
    }

    @Test
    fun `delete entity attribute should return a 404 if the entity is not found`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any(), MOCK_USER_SUB)
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                      {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title": "Entity urn:ngsi-ld:BeeHive:TESTC was not found",
                        "detail": "$DEFAULT_DETAIL"
                      }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 404 if the attribute is not found`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any(), MOCK_USER_SUB)
        } throws ResourceNotFoundException("Attribute Not Found")

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                      {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title": "Attribute Not Found",
                        "detail": "$DEFAULT_DETAIL"
                      }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 400 if the request is not correct`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any(), MOCK_USER_SUB)
        } returns BadRequestDataException("Something is wrong with the request").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST)
            .expectBody().json(
                """
                {
                  "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                  "title": "Something is wrong with the request",
                  "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 403 if user is not allowed to update entity`() {
        coEvery {
            entityService.deleteAttribute(any(), any(), any(), any(), sub.getOrNull())
        } returns AccessDeniedException("User forbidden write access to entity $beehiveId").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "User forbidden write access to entity urn:ngsi-ld:BeeHive:TESTC",
                    "detail": "$DEFAULT_DETAIL"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `replace attribute should return a 204 if attribute has been successfully replaced`() {
        val attributeFragment = ClassPathResource("/ngsild/fragments/beehive_new_incoming_property.json")
        coEvery {
            entityService.replaceAttribute(any(), any(), any())
        } returns UpdateResult(
            updated = listOf(INCOMING_PROPERTY),
            notUpdated = emptyList()
        ).right()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$INCOMING_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(attributeFragment)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityService.replaceAttribute(
                beehiveId,
                match {
                    it.first == INCOMING_PROPERTY
                },
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
            )
        }
    }

    @Test
    fun `replace attribute should return a 404 if the attribute does not exist`() {
        val attributeFragment = ClassPathResource("/ngsild/fragments/beehive_new_incoming_property.json")
        coEvery {
            entityService.replaceAttribute(any(), any(), any())
        } returns UpdateResult(
            updated = emptyList(),
            notUpdated = arrayListOf(
                NotUpdatedDetails(
                    INCOMING_PROPERTY,
                    "Unknown attribute $INCOMING_PROPERTY in entity $beehiveId"
                )
            )
        ).right()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$INCOMING_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(attributeFragment)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                      "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title": "Unknown attribute $INCOMING_PROPERTY in entity $beehiveId",
                      "detail": "$DEFAULT_DETAIL"
                    }
                    """
            )
    }
}
