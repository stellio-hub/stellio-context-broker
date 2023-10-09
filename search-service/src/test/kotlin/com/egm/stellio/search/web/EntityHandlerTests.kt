package com.egm.stellio.search.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.search.model.*
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_TIME_TYPE
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.Job
import org.hamcrest.core.Is
import org.junit.jupiter.api.BeforeAll
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
import java.net.URI
import java.time.*

@ActiveProfiles("test")
@WebFluxTest(EntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class EntityHandlerTests {

    private val aquacHeaderLink = buildContextLinkHeader(AQUAC_COMPOUND_CONTEXT)

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var queryService: QueryService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @MockkBean
    private lateinit var entityEventService: EntityEventService

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

    private val beehiveId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val breedingServiceType = "https://ontology.eglobalmark.com/aquac#BreedingService"
    private val deadFishesType = "https://ontology.eglobalmark.com/aquac#DeadFishes"
    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
    private val fishSizeAttribute = "https://ontology.eglobalmark.com/aquac#fishSize"
    private val hcmrContext = listOf(
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/shared-jsonld-contexts/egm.jsonld",
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/aquac/jsonld-contexts/aquac.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
    )

    @Test
    fun `create entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery { entityPayloadService.checkEntityExistence(any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.createEntity(any<NgsiLdEntity>(), any(), any())
        } returns Unit.right()
        coEvery { authorizationService.createAdminRight(any(), any()) } returns Unit.right()
        coEvery { entityEventService.publishEntityCreateEvent(any(), any(), any(), any()) } returns Job()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/$breedingServiceId"))

        coVerify {
            authorizationService.userCanCreateEntities(sub)
            entityPayloadService.checkEntityExistence(any(), true)
            entityPayloadService.createEntity(
                match<NgsiLdEntity> {
                    it.id == breedingServiceId
                },
                any(),
                any()
            )
            authorizationService.createAdminRight(eq(breedingServiceId), sub)
        }
        coVerify {
            entityEventService.publishEntityCreateEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(breedingServiceId),
                eq(listOf(breedingServiceType)),
                eq(hcmrContext)
            )
        }
    }

    @Test
    fun `create entity should return a 409 if the entity already exists`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery {
            entityPayloadService.checkEntityExistence(any(), any())
        } returns AlreadyExistsException("Already Exists").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(409)
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/AlreadyExists\"," +
                    "\"title\":\"The referred element already exists\"," +
                    "\"detail\":\"Already Exists\"}"
            )

        verify { entityEventService wasNot called }
    }

    @Test
    fun `create entity should return a 500 error if there is an internal server error`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery { entityPayloadService.checkEntityExistence(any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.createEntity(any<NgsiLdEntity>(), any(), any())
        } throws InternalErrorException("Internal Server Exception")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(500)
            .expectBody().json(
                """
                    {
                      "type":"https://uri.etsi.org/ngsi-ld/errors/InternalError",
                      "title":"There has been an error during the operation execution",
                      "detail":"InternalErrorException(message=Internal Server Exception)"
                    }
                    """
            )
    }

    @Test
    fun `create entity should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.LINK, aquacHeaderLink)
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
            .header(HttpHeaders.LINK, aquacHeaderLink)
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
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .bodyValue(entityWithoutType)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if creation unexpectedly fails`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery { entityPayloadService.checkEntityExistence(any(), any()) } returns Unit.right()
        // reproduce the runtime behavior where the raised exception is wrapped in an UndeclaredThrowableException
        coEvery {
            entityPayloadService.createEntity(any<NgsiLdEntity>(), any(), any())
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
                    "title": "The request includes input data which does not meet the requirements of the operation",
                    "detail": "Target entity does not exist"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `create entity should return a 403 if user is not allowed to create entities`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        coEvery {
            authorizationService.userCanCreateEntities(sub)
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
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden to create entities"
                }
                """.trimIndent()
            )
    }

    private fun mockkDefaultBehaviorForGetEntityById() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery { authorizationService.userCanReadEntity(beehiveId, sub) } returns Unit.right()
    }

    @Test
    fun `get entity by id should return 200 when entity exists`() {
        mockkDefaultBehaviorForGetEntityById()

        val returnedJsonLdEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        coEvery { queryService.queryEntity(any(), any()) } returns returnedJsonLdEntity.right()
        every { returnedJsonLdEntity.checkContainsAnyOf(any()) } returns Unit.right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `get entity by id should correctly serialize temporal properties`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                NGSILD_CREATED_AT_PROPERTY to
                    mapOf(
                        "@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                    ),
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly filter the asked attributes`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive"),
                "https://uri.etsi.org/ngsi-ld/default-context/attr1" to mapOf(
                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
                    NGSILD_PROPERTY_VALUE to mapOf(
                        "@value" to "some value 1"
                    )
                ),
                "https://uri.etsi.org/ngsi-ld/default-context/attr2" to mapOf(
                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
                    NGSILD_PROPERTY_VALUE to mapOf(
                        "@value" to "some value 2"
                    )
                )
            ),
            listOf(NGSILD_CORE_CONTEXT)
        ).right()

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
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive"),
                "https://uri.etsi.org/ngsi-ld/default-context/prop1" to mapOf(
                    JSONLD_TYPE to NGSILD_PROPERTY_TYPE.uri,
                    NGSILD_PROPERTY_VALUE to mapOf(
                        JSONLD_VALUE to "some value"
                    )
                ),
                "https://uri.etsi.org/ngsi-ld/default-context/rel1" to mapOf(
                    JSONLD_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                    NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(
                        JSONLD_ID to "urn:ngsi-ld:Entity:1234"
                    )
                )
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should return 404 if the entity has none of the requested attributes`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf(BEEHIVE_TYPE)
            ),
            listOf(NGSILD_CORE_CONTEXT)
        ).right()

        val expectedMessage = entityOrAttrsNotFoundMessage(
            beehiveId.toString(),
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
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"$expectedMessage"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should not include temporal properties if optional query param sysAttrs is not present`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("""{"@context":["$NGSILD_CORE_CONTEXT"]}""")
            .jsonPath("$.createdAt").doesNotExist()
            .jsonPath("$.modifiedAt").doesNotExist()
    }

    @Test
    fun `get entity by id should correctly serialize properties of type DateTime and display sysAttrs asked`() {
        mockkDefaultBehaviorForGetEntityById()
        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                NGSILD_CREATED_AT_PROPERTY to
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
                    NGSILD_CREATED_AT_PROPERTY to
                        mapOf(
                            "@type" to NGSILD_DATE_TIME_TYPE,
                            "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                        ),
                    NGSILD_MODIFIED_AT_PROPERTY to
                        mapOf(
                            "@type" to NGSILD_DATE_TIME_TYPE,
                            "@value" to Instant.parse("2015-10-18T12:20:30.000001Z").atZone(ZoneOffset.UTC)
                        )
                ),
                "@id" to beehiveId.toString(),
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                        "type":"Property",
                        "value":{
                            "type":"DateTime",
                            "@value":"2015-10-18T11:20:30.000001Z"
                        },
                        "createdAt":"2015-10-18T11:20:30.000001Z",
                        "modifiedAt":"2015-10-18T12:20:30.000001Z"
                    },
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Date`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
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
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                        "type":"Property",
                        "value":{
                            "type":"Date",
                            "@value":"2015-10-18"
                        }
                    },
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize properties of type Time`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
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
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                        "type":"Property",
                        "value":{
                            "type":"Time",
                            "@value":"11:20:30"
                        }
                    },
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute property having one instance`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/name" to
                    mapOf(
                        JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Property",
                        NGSILD_PROPERTY_VALUE to "ruche",
                        NGSILD_DATASET_ID_PROPERTY to mapOf(
                            JSONLD_ID to "urn:ngsi-ld:Property:french-name"
                        )
                    ),
                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                    "type":"Beehive",
                    "name":{"type":"Property","datasetId":"urn:ngsi-ld:Property:french-name","value":"ruche"},
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute property having more than one instance`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/name" to
                    listOf(
                        mapOf(
                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Property",
                            NGSILD_PROPERTY_VALUE to "beehive",
                            NGSILD_DATASET_ID_PROPERTY to mapOf(
                                JSONLD_ID to "urn:ngsi-ld:Property:english-name"
                            )
                        ),
                        mapOf(
                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Property",
                            NGSILD_PROPERTY_VALUE to "ruche",
                            NGSILD_DATASET_ID_PROPERTY to mapOf(
                                JSONLD_ID to "urn:ngsi-ld:Property:french-name"
                            )
                        )
                    ),
                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                    "type":"Beehive",
                    "name":[
                        {
                            "type":"Property","datasetId":"urn:ngsi-ld:Property:english-name","value":"beehive"
                        },
                        {
                            "type":"Property","datasetId":"urn:ngsi-ld:Property:french-name","value":"ruche"
                        }
                    ],
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute relationship having one instance`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
                    mapOf(
                        JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Relationship",
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(
                            JSONLD_ID to "urn:ngsi-ld:Beekeeper:1230"
                        ),
                        NGSILD_DATASET_ID_PROPERTY to mapOf(
                            JSONLD_ID to "urn:ngsi-ld:Dataset:managedBy:0215"
                        )
                    ),
                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                    "type":"Beehive",
                    "managedBy": {
                      "type":"Relationship",
                       "datasetId":"urn:ngsi-ld:Dataset:managedBy:0215",
                        "object":"urn:ngsi-ld:Beekeeper:1230"
                    },
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should include createdAt & modifiedAt if query param sysAttrs is present`() {
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
                    mapOf(
                        JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Relationship",
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(
                            JSONLD_ID to "urn:ngsi-ld:Beekeeper:1230"
                        ),
                        NGSILD_DATASET_ID_PROPERTY to mapOf(
                            JSONLD_ID to "urn:ngsi-ld:Dataset:managedBy:0215"
                        ),
                        NGSILD_CREATED_AT_PROPERTY to
                            mapOf(
                                "@type" to NGSILD_DATE_TIME_TYPE,
                                "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                            ),
                        NGSILD_MODIFIED_AT_PROPERTY to
                            mapOf(
                                "@type" to NGSILD_DATE_TIME_TYPE,
                                "@value" to Instant.parse("2015-10-18T12:20:30.000001Z").atZone(ZoneOffset.UTC)
                            )
                    ),
                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
        mockkDefaultBehaviorForGetEntityById()

        coEvery { queryService.queryEntity(any(), any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/managedBy" to
                    listOf(
                        mapOf(
                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Relationship",
                            NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(
                                JSONLD_ID to "urn:ngsi-ld:Beekeeper:1229"
                            )
                        ),
                        mapOf(
                            JSONLD_TYPE to "https://uri.etsi.org/ngsi-ld/Relationship",
                            NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(
                                JSONLD_ID to "urn:ngsi-ld:Beekeeper:1230"
                            ),
                            NGSILD_DATASET_ID_PROPERTY to mapOf(
                                JSONLD_ID to "urn:ngsi-ld:Dataset:managedBy:0215"
                            )
                        )
                    ),
                JSONLD_ID to "urn:ngsi-ld:Beehive:4567",
                JSONLD_TYPE to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
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
                    "type":"Beehive",
                    "managedBy":[
                       {
                          "type":"Relationship",
                          "object":"urn:ngsi-ld:Beekeeper:1229"
                       },
                       {
                          "type":"Relationship",
                          "datasetId":"urn:ngsi-ld:Dataset:managedBy:0215",
                          "object":"urn:ngsi-ld:Beekeeper:1230"
                       }
                    ],
                    "@context": ["$NGSILD_CORE_CONTEXT"]
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should return 404 when entity does not exist`() {
        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns ResourceNotFoundException(entityNotFoundMessage("urn:ngsi-ld:BeeHive:TEST")).left()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"${entityNotFoundMessage("urn:ngsi-ld:BeeHive:TEST")}\"}"
            )
    }

    @Test
    fun `get entity by id should return 403 if user is not authorized to read an entity`() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery {
            authorizationService.userCanReadEntity("urn:ngsi-ld:BeeHive:TEST".toUri(), sub)
        } returns AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:BeeHive:TEST").left()

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden read access to entity urn:ngsi-ld:BeeHive:TEST"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities by type should not include temporal properties if query param sysAttrs is not present`() {
        coEvery { queryService.queryEntities(any(), any()) } returns Pair(
            listOf(
                JsonLdEntity(
                    mapOf(
                        "@id" to beehiveId.toString(),
                        "@type" to listOf("Beehive")
                    ),
                    listOf(NGSILD_CORE_CONTEXT)
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
                        "@context": ["$NGSILD_CORE_CONTEXT"]
                    }
                ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get entities by type should include temporal properties if optional query param sysAttrs is present`() {
        coEvery {
            queryService.queryEntities(
                QueryParams(
                    type = "https://uri.etsi.org/ngsi-ld/default-context/Beehive",
                    limit = 30,
                    offset = 0,
                    includeSysAttrs = true,
                    context = NGSILD_CORE_CONTEXT
                ),
                any()
            )
        } returns Pair(
            listOf(
                JsonLdEntity(
                    mapOf(
                        NGSILD_CREATED_AT_PROPERTY to
                            mapOf(
                                "@type" to NGSILD_DATE_TIME_TYPE,
                                "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                            ),
                        "@id" to beehiveId.toString(),
                        "@type" to listOf("Beehive")
                    ),
                    listOf(NGSILD_CORE_CONTEXT)
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
                        "@context": ["$NGSILD_CORE_CONTEXT"]
                    }
                ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entities should return 200 with prev and next link header if exists`() {
        coEvery { queryService.queryEntities(any(), any()) } returns Pair(
            listOf(
                JsonLdEntity(
                    mapOf("@id" to "urn:ngsi-ld:Beehive:TESTC", "@type" to listOf("Beehive")),
                    listOf(NGSILD_CORE_CONTEXT)
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
                "</ngsi-ld/v1/entities?type=Beehive&id=urn:ngsi-ld:Beehive:TESTC,urn:ngsi-ld:Beehive:TESTB," +
                    "urn:ngsi-ld:Beehive:TESTD&limit=1&offset=0>;rel=\"prev\";type=\"application/ld+json\"",
                "</ngsi-ld/v1/entities?type=Beehive&id=urn:ngsi-ld:Beehive:TESTC,urn:ngsi-ld:Beehive:TESTB," +
                    "urn:ngsi-ld:Beehive:TESTD&limit=1&offset=2>;rel=\"next\";type=\"application/ld+json\""
            )
            .expectBody().json(
                """
                [
                    {
                        "id": "urn:ngsi-ld:Beehive:TESTC",
                        "type": "Beehive",
                        "@context": ["$NGSILD_CORE_CONTEXT"]
                    }
                ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entities should return 200 and empty response if requested offset does not exists`() {
        coEvery {
            queryService.queryEntities(any(), any())
        } returns Pair(emptyList<JsonLdEntity>(), 0).right()

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
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Offset must be greater than zero and limit must be strictly greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities should return 400 if limit is greater than the maximum authorized limit`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities/?type=Beehive&limit=200&offset=1")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"You asked for 200 results, but the supported maximum limit is 100"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities with request parameter id should return 200`() {
        coEvery {
            queryService.queryEntities(
                QueryParams(
                    ids = setOf(beehiveId),
                    limit = 30,
                    offset = 0,
                    context = NGSILD_CORE_CONTEXT
                ),
                any()
            )
        } returns Pair(
            listOf(
                JsonLdEntity(
                    mapOf(
                        "@id" to beehiveId.toString(),
                        "@type" to listOf("Beehive")
                    ),
                    listOf(NGSILD_CORE_CONTEXT)
                )
            ),
            1
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entities?id=$beehiveId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                [
                    {
                        "id": "$beehiveId",
                        "type": "Beehive",
                        "@context": ["$NGSILD_CORE_CONTEXT"]
                    }
                ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entities should return 200 and the number of results`() {
        coEvery { queryService.queryEntities(any(), any()) } returns Pair(emptyList<JsonLdEntity>(), 3).right()

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
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Offset and limit must be greater than zero"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `get entities should allow a query not including a type request parameter`() {
        coEvery { queryService.queryEntities(any(), any()) } returns Pair(emptyList<JsonLdEntity>(), 0).right()

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
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"one of 'ids', 'q', 'type' and 'attrs' request parameters have to be specified"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `replace entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery { entityPayloadService.checkEntityExistence(breedingServiceId) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(breedingServiceId, sub) } returns Unit.right()
        coEvery {
            entityPayloadService.replaceEntity(any(), any<NgsiLdEntity>(), any(), any())
        } returns Unit.right()
        coEvery { entityEventService.publishEntityReplaceEvent(any(), any(), any(), any()) } returns Job()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanUpdateEntity(breedingServiceId, sub)
            entityPayloadService.checkEntityExistence(breedingServiceId)
            entityPayloadService.replaceEntity(
                eq(breedingServiceId),
                match<NgsiLdEntity> {
                    it.id == breedingServiceId
                },
                any(),
                any()
            )
        }
        coVerify {
            entityEventService.publishEntityReplaceEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(breedingServiceId),
                eq(listOf(breedingServiceType)),
                eq(hcmrContext)
            )
        }
    }

    @Test
    fun `replace entity should return a 403 if user is not allowed to update the entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery { entityPayloadService.checkEntityExistence(breedingServiceId) } returns Unit.right()
        coEvery {
            authorizationService.userCanUpdateEntity(breedingServiceId, sub)
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
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden to modify entity"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `replace entity should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityPayloadService.checkEntityExistence(eq(breedingServiceId))
        } returns ResourceNotFoundException(entityNotFoundMessage(breedingServiceId.toString())).left()
        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        coVerify { entityPayloadService.checkEntityExistence(eq(breedingServiceId)) }
    }

    @Test
    fun `replace entity should return a 400 if id contained in payload is different from the one in URL`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0215".toUri()

        coEvery { entityPayloadService.checkEntityExistence(breedingServiceId) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(breedingServiceId, sub) } returns Unit.right()

        webClient.put()
            .uri("/ngsi-ld/v1/entities/$breedingServiceId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"The id contained in the body is not the same as the one provided in the URL"
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
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Missing entity id when trying to replace an entity"
                }
                """.trimIndent()
            )
    }

    private fun mockkDefaultBehaviorForAppendAttribute() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(breedingServiceType).right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), sub)
        } returns Unit.right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any())
        } returns Job()
    }

    @Test
    fun `append entity attribute should return a 204 if all attributes were appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendResult = UpdateResult(
            listOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.APPENDED
                )
            ),
            emptyList()
        )

        mockkDefaultBehaviorForAppendAttribute()
        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
        } returns appendResult.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.checkEntityExistence(eq(entityId))
            authorizationService.userCanUpdateEntity(eq(entityId), eq(sub))
            entityPayloadService.appendAttributes(
                eq(entityId),
                any(),
                eq(false),
                sub.getOrNull()
            )
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                appendResult,
                true,
                listOf(AQUAC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `append entity attribute should return a 207 if some attributes could not be appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_twoNewProperties.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendResult = UpdateResult(
            listOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.APPENDED
                )
            ),
            listOf(NotUpdatedDetails(fishSizeAttribute, "overwrite disallowed"))
        )

        mockkDefaultBehaviorForAppendAttribute()
        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
        } returns appendResult.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
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
            entityPayloadService.checkEntityExistence(eq(entityId))
            entityPayloadService.appendAttributes(
                eq(entityId),
                any(),
                eq(false),
                sub.getOrNull()
            )
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                appendResult,
                true,
                listOf(AQUAC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `append entity attribute should return a 204 when adding a new type to an entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newType.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendTypeResult = UpdateResult(
            listOf(UpdatedDetails(JSONLD_TYPE, null, UpdateOperationResult.APPENDED)),
            emptyList()
        )

        mockkDefaultBehaviorForAppendAttribute()
        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
        } returns appendTypeResult.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.NO_CONTENT)

        coVerify {
            entityPayloadService.checkEntityExistence(eq(entityId))
            entityPayloadService.appendAttributes(
                eq(entityId),
                any(),
                eq(false),
                any()
            )
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                appendTypeResult,
                true,
                listOf(AQUAC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `append entity attribute should return a 207 if types or attributes could not be appended`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newInvalidTypeAndAttribute.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val appendTypeResult = UpdateResult(
            emptyList(),
            listOf(NotUpdatedDetails(JSONLD_TYPE, "Append operation has unexpectedly failed"))
        )
        val appendResult = UpdateResult(
            listOf(UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.APPENDED)),
            listOf(NotUpdatedDetails(fishSizeAttribute, "overwrite disallowed"))
        )

        mockkDefaultBehaviorForAppendAttribute()
        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
        } returns appendTypeResult.mergeWith(appendResult).right()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
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

        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                appendTypeResult.mergeWith(appendResult),
                true,
                listOf(AQUAC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `append entity attribute should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery {
            entityPayloadService.checkEntityExistence(eq(entityId))
        } returns ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity urn:ngsi-ld:BreedingService:0214 was not found\"}"
            )

        coVerify {
            entityPayloadService.checkEntityExistence(eq(entityId))
        }
        verify {
            entityEventService wasNot called
        }
    }

    @Test
    @SuppressWarnings("MaxLineLength")
    fun `append entity attribute should return a 400 if the attribute is not NGSI-LD valid`() {
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()
        val invalidPayload =
            """
            {
                "connectsTo":{
                    "type":"Relationship"
                }
            }
            """.trimIndent()

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.appendAttributes(any(), any(), any(), any())
        } returns BadRequestDataException(
            "Relationship https://ontology.eglobalmark.com/egm#connectsTo does not have an object field"
        ).left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(invalidPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "The request includes input data which does not meet the requirements of the operation",
                    "detail": "Relationship https://ontology.eglobalmark.com/egm#connectsTo does not have an object field"
                } 
                """.trimIndent()
            )
    }

    @Test
    fun `append entity attribute should return a 403 if user is not allowed to update entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(breedingServiceType).right()
        coEvery {
            authorizationService.userCanUpdateEntity(entityId, sub)
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:BreedingService:0214").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                { 
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied", 
                    "title": "The request tried to access an unauthorized resource", 
                    "detail": "User forbidden write access to entity urn:ngsi-ld:BreedingService:0214" 
                } 
                """.trimIndent()
            )

        verify { entityEventService wasNot called }
    }

    private fun mockkDefaultBehaviorForPartialUpdateAttribute() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(deadFishesType).right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
    }

    @Test
    fun `partial attribute update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = loadSampleData("aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNumberAttribute, "urn:ngsi-ld:Dataset:1".toUri(), UpdateOperationResult.UPDATED)
            ),
            notUpdated = arrayListOf()
        )

        mockkDefaultBehaviorForPartialUpdateAttribute()
        coEvery {
            entityPayloadService.partialUpdateAttribute(any(), any(), any())
        } returns updateResult.right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any(), any())
        } returns Job()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.checkEntityExistence(eq(entityId))
            authorizationService.userCanUpdateEntity(eq(entityId), eq(sub))
            entityPayloadService.partialUpdateAttribute(eq(entityId), any(), sub.getOrNull())
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                eq(updateResult),
                eq(false),
                eq(listOf(AQUAC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `partial attribute update should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        coVerify { entityPayloadService.checkEntityExistence(eq(entityId)) }
    }

    @Test
    fun `partial attribute update should return a 404 if attribute does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        mockkDefaultBehaviorForPartialUpdateAttribute()
        coEvery {
            entityPayloadService.partialUpdateAttribute(any(), any(), any())
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
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        coVerify {
            entityPayloadService.partialUpdateAttribute(eq(entityId), any(), sub.getOrNull())
        }
    }

    @Test
    fun `partial attribute update should not authorize user without write rights on entity to update attribute`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(deadFishesType).right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), sub)
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                { 
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied", 
                    "title": "The request tried to access an unauthorized resource", 
                    "detail": "User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN" 
                } 
                """.trimIndent()
            )

        coVerify { authorizationService.userCanUpdateEntity(eq(entityId), sub) }
    }

    private fun mockkDefaultBehaviorForUpdateAttribute() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(deadFishesType).right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
    }

    @Test
    fun `merge entity should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.REPLACED
                ),
                UpdatedDetails(
                    fishSizeAttribute,
                    null,
                    UpdateOperationResult.APPENDED
                )
            ),
            notUpdated = emptyList()
        )

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.mergeEntity(any(), any(), any(), any())
        } returns updateResult.right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any())
        } returns Job()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.checkEntityExistence(eq(entityId))
            authorizationService.userCanUpdateEntity(eq(entityId), eq(sub))
            entityPayloadService.mergeEntity(eq(entityId), any(), any(), any())
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                eq(updateResult),
                true,
                eq(listOf(AQUAC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `merge entity should return a 204 if JSON-LD payload is correct and use observedAt parameter`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.REPLACED
                ),
                UpdatedDetails(
                    fishSizeAttribute,
                    null,
                    UpdateOperationResult.APPENDED
                )
            ),
            notUpdated = emptyList()
        )

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.mergeEntity(any(), any(), any(), any())
        } returns updateResult.right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any())
        } returns Job()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId?observedAt=2019-12-04T12:00:00.00Z")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.checkEntityExistence(eq(entityId))
            authorizationService.userCanUpdateEntity(eq(entityId), eq(sub))
            entityPayloadService.mergeEntity(
                eq(entityId),
                any(),
                eq(ZonedDateTime.parse("2019-12-04T12:00:00.00Z")),
                any()
            )
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                eq(updateResult),
                true,
                eq(listOf(AQUAC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `merge entity should return a 400 if optional parameter observedAt is not a datetime`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId?observedAt=notDateTime")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'observedAt' parameter is not a valid date"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                  "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                  "title":"The referred resource has not been found",
                  "detail":"${entityNotFoundMessage(entityId.toString())}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 403 if user is not allowed to update it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden write access to entity urn:ngsi-ld:DeadFishes:019BN"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `merge entity should return a 400 if entityId is missing`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_mergeEntity.json")

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Missing entity id when trying to merge an entity"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `entity attributes update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updateResult = UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.REPLACED
                )
            ),
            notUpdated = emptyList()
        )

        mockkDefaultBehaviorForUpdateAttribute()
        coEvery {
            entityPayloadService.updateAttributes(any(), any(), any())
        } returns updateResult.right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any())
        } returns Job()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.checkEntityExistence(eq(entityId))
            authorizationService.userCanUpdateEntity(eq(entityId), eq(sub))
            entityPayloadService.updateAttributes(eq(entityId), any(), any())
        }
        coVerify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityId),
                any(),
                eq(updateResult),
                true,
                eq(listOf(AQUAC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `entity attributes update should return a 207 if some relationships objects are not found`() {
        val jsonLdFile = ClassPathResource(
            "/ngsild/aquac/fragments/DeadFishes_updateEntityAttributes_invalidAttribute.json"
        )
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val notUpdatedAttribute = NotUpdatedDetails("removedFrom", "Property is not valid")

        mockkDefaultBehaviorForUpdateAttribute()
        coEvery {
            entityPayloadService.updateAttributes(any(), any(), any())
        } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(fishNumberAttribute, null, UpdateOperationResult.REPLACED)
            ),
            notUpdated = arrayListOf(notUpdatedAttribute)
        ).right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any())
        } returns Job()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
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

        mockkDefaultBehaviorForUpdateAttribute()
        coEvery {
            entityPayloadService.updateAttributes(any(), any(), any())
        } returns UpdateResult(
            updated = emptyList(),
            notUpdated = listOf(NotUpdatedDetails("type", "A type cannot be removed"))
        ).right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any())
        } returns Job()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)

        coVerify {
            entityPayloadService.updateAttributes(eq(entityId), any(), any())
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
                    "title": "A remote JSON-LD @context referenced in a request cannot be retrieved by the NGSI-LD Broker and expansion or compaction cannot be performed",
                    "detail": "Unable to load remote context (cause was: com.github.jsonldjava.core.JsonLdError: loading remote context failed: https://easyglobalmarket.com/contexts/diat.jsonld)"
                }
                """.trimIndent()
            )

        verify { entityEventService wasNot called }
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
            entityPayloadService.checkEntityExistence(any())
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs")
            .bodyValue(payload)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity $beehiveId was not found\"}"
            )
    }

    @Test
    fun `entity attributes update should return a 403 if user is not allowed to update it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(deadFishesType).right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:Sensor:0022CCC").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden write access to entity urn:ngsi-ld:Sensor:0022CCC"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity should return a 204 if an entity has been successfully deleted`() {
        val entity = mockkClass(EntityPayload::class, relaxed = true)

        coEvery { entityPayloadService.checkEntityExistence(beehiveId) } returns Unit.right()
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns entity.right()
        every { entity.types } returns listOf(BEEHIVE_TYPE)
        every { entity.contexts } returns listOf(APIC_COMPOUND_CONTEXT)
        coEvery { authorizationService.userCanAdminEntity(beehiveId, sub) } returns Unit.right()
        coEvery { entityPayloadService.deleteEntity(any()) } returns Unit.right()
        coEvery { authorizationService.removeRightsOnEntity(any()) } returns Unit.right()
        coEvery { entityEventService.publishEntityDeleteEvent(any(), any(), any(), any()) } returns Job()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityPayloadService.checkEntityExistence(beehiveId)
            entityPayloadService.retrieve(eq(beehiveId))
            authorizationService.userCanAdminEntity(eq(beehiveId), eq(sub))
            entityPayloadService.deleteEntity(eq(beehiveId))
            authorizationService.removeRightsOnEntity(eq(beehiveId))
        }
        coVerify {
            entityEventService.publishEntityDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(beehiveId),
                eq(listOf(BEEHIVE_TYPE)),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `delete entity should return a 404 if entity to be deleted has not been found`() {
        coEvery {
            entityPayloadService.checkEntityExistence(beehiveId)
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                  "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                  "title":"The referred resource has not been found",
                  "detail":"${entityNotFoundMessage(beehiveId.toString())}"
                }
                """.trimIndent()
            )

        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete entity should return a 500 if entity could not be deleted`() {
        val entity = mockkClass(EntityPayload::class, relaxed = true)
        coEvery { entityPayloadService.checkEntityExistence(beehiveId) } returns Unit.right()
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns entity.right()
        every { entity.types } returns listOf(BEEHIVE_TYPE)
        coEvery { authorizationService.userCanAdminEntity(beehiveId, sub) } returns Unit.right()
        coEvery {
            entityPayloadService.deleteEntity(any())
        } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$beehiveId")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                """
                    {
                      "type":"https://uri.etsi.org/ngsi-ld/errors/InternalError",
                      "title":"There has been an error during the operation execution",
                      "detail":"java.lang.RuntimeException: Unexpected server error"
                    }
                    """
            )
    }

    @Test
    fun `delete entity should return a 403 is user is not authorized to delete an entity`() {
        val entity = mockkClass(EntityPayload::class, relaxed = true)
        coEvery { entityPayloadService.checkEntityExistence(beehiveId) } returns Unit.right()
        coEvery { entityPayloadService.retrieve(beehiveId) } returns entity.right()
        every { entity.types } returns listOf(BEEHIVE_TYPE)
        coEvery {
            authorizationService.userCanAdminEntity(beehiveId, sub)
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
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden admin access to entity $beehiveId"
                }
                """.trimIndent()
            )
    }

    private fun mockkDefaultBehaviorForDeleteAttribute() {
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any(), any())
        } returns Job()
    }

    @Test
    fun `delete entity attribute should return a 204 if the attribute has been successfully deleted`() {
        mockkDefaultBehaviorForDeleteAttribute()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            authorizationService.userCanUpdateEntity(eq(beehiveId), eq(sub))
            entityPayloadService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                null
            )
        }
        coVerify {
            entityEventService.publishAttributeDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                isNull(),
                eq(false),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `delete entity attribute should delete all instances if deleteAll flag is true`() {
        mockkDefaultBehaviorForDeleteAttribute()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityPayloadService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                null,
                eq(true)
            )
        }
        coVerify {
            entityEventService.publishAttributeDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                isNull(),
                eq(true),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `delete entity attribute should delete instance with the provided datasetId`() {
        val datasetId = "urn:ngsi-ld:Dataset:temperature:1"
        mockkDefaultBehaviorForDeleteAttribute()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY?datasetId=$datasetId")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityPayloadService.deleteAttribute(
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                eq(datasetId.toUri())
            )
        }
        coVerify {
            entityEventService.publishAttributeDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(beehiveId),
                eq(TEMPERATURE_PROPERTY),
                eq(datasetId.toUri()),
                eq(false),
                eq(listOf(APIC_COMPOUND_CONTEXT))
            )
        }
    }

    @Test
    fun `delete entity attribute should return a 404 if the entity is not found`() {
        mockkDefaultBehaviorForDeleteAttribute()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(beehiveId.toString())).left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity urn:ngsi-ld:BeeHive:TESTC was not found\"}"
            )

        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete entity attribute should return a 404 if the attribute is not found`() {
        mockkDefaultBehaviorForDeleteAttribute()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any(), any())
        } throws ResourceNotFoundException("Attribute Not Found")

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$beehiveId/attrs/$TEMPERATURE_COMPACT_PROPERTY?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Attribute Not Found\"}"
            )

        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete entity attribute should return a 400 if the request is not correct`() {
        mockkDefaultBehaviorForDeleteAttribute()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any())
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
                  "title": "The request includes input data which does not meet the requirements of the operation",
                  "detail": "Something is wrong with the request"
                }
                """.trimIndent()
            )

        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete entity attribute should return a 403 if user is not allowed to update entity`() {
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), sub)
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
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden write access to entity urn:ngsi-ld:BeeHive:TESTC"
                }
                """.trimIndent()
            )
        verify { entityEventService wasNot called }
    }

    private fun mockkDefaultBehaviorForReplaceAttribute() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any(), any())
        } returns Job()
    }

    @Test
    fun `replace attribute should return a 204 if attribute has been successfully replaced`() {
        mockkDefaultBehaviorForReplaceAttribute()
        val attributeFragment = ClassPathResource("/ngsild/fragments/beehive_new_incoming_property.json")
        coEvery {
            entityPayloadService.replaceAttribute(any(), any(), any())
        } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(INCOMING_PROPERTY, null, UpdateOperationResult.REPLACED)
            ),
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
            entityPayloadService.replaceAttribute(
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
        mockkDefaultBehaviorForReplaceAttribute()
        val attributeFragment = ClassPathResource("/ngsild/fragments/beehive_new_incoming_property.json")
        coEvery {
            entityPayloadService.replaceAttribute(any(), any(), any())
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
                      "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                      "title":"The referred resource has not been found",
                      "detail":"Unknown attribute $INCOMING_PROPERTY in entity $beehiveId"
                    }
                    """
            )
    }
}
