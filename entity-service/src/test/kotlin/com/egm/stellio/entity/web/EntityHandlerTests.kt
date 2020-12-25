package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.service.EntityAttributeService
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
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
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.hamcrest.core.Is
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.lang.reflect.UndeclaredThrowableException
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset

@ActiveProfiles("test")
@WebFluxTest(EntityHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", username = "mock-user")
class EntityHandlerTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    private lateinit var aquacHeaderLink: String

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var entityAttributeService: EntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @MockkBean
    private lateinit var entityEventService: EntityEventService

    @BeforeAll
    fun configureWebClientDefaults() {
        aquacHeaderLink = "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"

        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    private val fishNumberAttribute = "https://ontology.eglobalmark.com/aquac#fishNumber"
    private val hcmrContext = listOf(
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/shared-jsonld-contexts/egm.jsonld",
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/" +
            "master/aquac/jsonld-contexts/aquac.jsonld",
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    )

    @Test
    fun `create entity should return a 201 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")
        val breedingServiceId = "urn:ngsi-ld:BreedingService:0214".toUri()

        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityService.createEntity(any()) } returns breedingServiceId
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/entities/$breedingServiceId"))

        verify { authorizationService.userCanCreateEntities("mock-user") }
        verify { authorizationService.createAdminLink(breedingServiceId, "mock-user") }
        verify {
            entityService.createEntity(
                match {
                    it.id == breedingServiceId
                }
            )
        }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as EntityCreateEvent
                    it.operationType == EventsType.ENTITY_CREATE &&
                        it.entityId == breedingServiceId &&
                        it.contexts == hcmrContext
                },
                "BreedingService"
            )
        }

        confirmVerified(entityService, authorizationService)
    }

    @Test
    fun `create entity should return a 409 if the entity already exists`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityService.createEntity(any()) } throws AlreadyExistsException("Already Exists")

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
    fun `create entity should return a 500 error if internal server Error`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        every { entityService.createEntity(any()) } throws InternalErrorException("Internal Server Exception")

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(500)
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"Internal Server Exception\"}"
            )
    }

    @Test
    fun `create entity should return a 400 if JSON-LD payload is not correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/beehive_missing_context.jsonld")

        every { authorizationService.userCanCreateEntities("mock-user") } returns true

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

        every { authorizationService.userCanCreateEntities("mock-user") } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.LINK, aquacHeaderLink)
            .bodyValue(entityWithoutId)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `create entity should return a 400 if entity does not have an type`() {
        every { authorizationService.userCanCreateEntities("mock-user") } returns true
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
    fun `create entity should return a 400 if input data is not valid and creation was rejected`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { authorizationService.userCanCreateEntities("mock-user") } returns true
        // reproduce the runtime behavior where the raised exception is wrapped in an UndeclaredThrowableException
        every {
            entityService.createEntity(any())
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
    fun `it should not authorize user without creator role to create entity`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/BreedingService.json")

        every { authorizationService.userCanCreateEntities("mock-user") } returns false

        webClient.post()
            .uri("/ngsi-ld/v1/entities")
            .header(HttpHeaders.LINK, aquacHeaderLink)
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

    @Test
    fun `get entity by id should return 200 when entity exists`() {
        val returnedJsonLdEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns returnedJsonLdEntity
        every { returnedJsonLdEntity.containsAnyOf(any()) } returns true

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `get entity by id should correctly serialize temporal properties`() {
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any(), true) } returns JsonLdEntity(
            mapOf(
                NGSILD_CREATED_AT_PROPERTY to
                    mapOf(
                        "@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                    ),
                "@id" to "urn:ngsi-ld:Beehive:TESTC",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?options=sysAttrs")
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
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any(), false) } returns JsonLdEntity(
            mapOf(
                "@id" to "urn:ngsi-ld:Beehive:TESTC",
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
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?attrs=attr2")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.attr1").doesNotExist()
            .jsonPath("$.attr2").isNotEmpty
    }

    @Test
    fun `get entity by id should correctly return the simplified representation of an entity`() {
        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any(), false) } returns JsonLdEntity(
            mapOf(
                "@id" to entityId.toString(),
                "@type" to listOf("Beehive"),
                "https://uri.etsi.org/ngsi-ld/default-context/prop1" to mapOf(
                    JSONLD_TYPE to NGSILD_PROPERTY_TYPE.uri,
                    NGSILD_PROPERTY_VALUE to mapOf(
                        JSONLD_VALUE_KW to "some value"
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
        )

        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?options=keyValues")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .json(
                """
                    {
                        "id": "$entityId",
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
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any(), false) } returns JsonLdEntity(
            mapOf(
                "@id" to "urn:ngsi-ld:Beehive:TESTC",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?attrs=attr2")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                        "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title":"The referred resource has not been found",
                        "detail":"Entity $entityId does not have any of the requested attributes"
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should not include temporal properties if optional query param sysAttrs is not present`() {
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns JsonLdEntity(
            mapOf(
                "@id" to "urn:ngsi-ld:Beehive:TESTC",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("""{"@context":["$NGSILD_CORE_CONTEXT"]}""")
            .jsonPath("$.createdAt").doesNotExist()
            .jsonPath("$.modifiedAt").doesNotExist()
    }

    @Test
    fun `get entities by type should not include temporal properties if query param sysAttrs is not present`() {
        every { entityService.exists(any()) } returns true
        every { entityService.searchEntities(any(), any(), any<String>(), false) } returns listOf(
            JsonLdEntity(
                mapOf(
                    "@id" to "urn:ngsi-ld:Beehive:TESTC",
                    "@type" to listOf("Beehive")
                ),
                listOf(NGSILD_CORE_CONTEXT)
            )
        )

        val entityId = "urn:ngsi-ld:Beehive:TESTC".toUri()
        every {
            authorizationService.filterEntitiesUserCanRead(any(), "mock-user")
        } returns listOf(entityId)

        webClient.get()
            .uri("/ngsi-ld/v1/entities?type=Beehive")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """[
                        {
                            "id": "urn:ngsi-ld:Beehive:TESTC",
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
        every { entityService.exists(any()) } returns true
        every { entityService.searchEntities(any(), any(), any<String>(), true) } returns listOf(
            JsonLdEntity(
                mapOf(
                    NGSILD_CREATED_AT_PROPERTY to
                        mapOf(
                            "@type" to NGSILD_DATE_TIME_TYPE,
                            "@value" to Instant.parse("2015-10-18T11:20:30.000001Z").atZone(ZoneOffset.UTC)
                        ),
                    "@id" to "urn:ngsi-ld:Beehive:TESTC",
                    "@type" to listOf("Beehive")
                ),
                listOf(NGSILD_CORE_CONTEXT)
            )
        )

        val entityId = "urn:ngsi-ld:Beehive:TESTC".toUri()
        every {
            authorizationService.filterEntitiesUserCanRead(any(), "mock-user")
        } returns listOf(entityId)

        webClient.get()
            .uri("/ngsi-ld/v1/entities?type=Beehive&options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """[
                        {
                            "id": "urn:ngsi-ld:Beehive:TESTC",
                            "type": "Beehive",
                            "createdAt":"2015-10-18T11:20:30.000001Z",
                            "@context": ["$NGSILD_CORE_CONTEXT"]
                        }
                    ]
                """.trimMargin()
            )
    }

    @Test
    fun `get entity by id should correctly serialize properties of type DateTime and display sysAttrs asked`() {
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any(), true) } returns JsonLdEntity(
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
                "@id" to "urn:ngsi-ld:Beehive:TESTC",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?options=sysAttrs")
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
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/default-context/testedAt" to mapOf(
                    "@type" to "https://uri.etsi.org/ngsi-ld/Property",
                    NGSILD_PROPERTY_VALUE to mapOf(
                        "@type" to NGSILD_DATE_TYPE,
                        "@value" to LocalDate.of(2015, 10, 18)
                    )
                ),
                "@id" to "urn:ngsi-ld:Beehive:TESTC",
                "@type" to listOf("Beehive")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
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
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns JsonLdEntity(
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
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
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
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/name" to
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
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
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
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns JsonLdEntity(
            mapOf(
                "https://uri.etsi.org/ngsi-ld/name" to
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
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
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
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns JsonLdEntity(
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
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """
                    {
                        "id":"urn:ngsi-ld:Beehive:4567",
                        "type":"Beehive",
                        "managedBy":{"type":"Relationship", "datasetId":"urn:ngsi-ld:Dataset:managedBy:0215", "object":"urn:ngsi-ld:Beekeeper:1230"},
                        "@context": ["$NGSILD_CORE_CONTEXT"]
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `get entity by id should include createdAt & modifiedAt if query param sysAttrs is present`() {
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any(), true) } returns JsonLdEntity(
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
        )

        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?options=sysAttrs")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").isEqualTo("2015-10-18T11:20:30.000001Z")
            .jsonPath("$..modifiedAt").isEqualTo("2015-10-18T12:20:30.000001Z")
    }

    @Test
    fun `get entity by id should correctly serialize multi-attribute relationship having more than one instance`() {
        every { entityService.exists(any()) } returns true
        every { entityService.getFullEntityById(any()) } returns JsonLdEntity(
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
        )
        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

        every { authorizationService.userCanReadEntity(entityId, "mock-user") } returns true

        webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId")
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
        every { entityService.exists(any()) } returns false

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
    fun `it should not authorize user without read rights on entity to get it`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.userCanReadEntity("urn:ngsi-ld:BeeHive:TEST".toUri(), "mock-user")
        } returns false

        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:BeeHive:TEST")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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
    fun `search on entities should return 400 if required parameters are missing`() {
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
                    "detail":"'q' or 'type' request parameters have to be specified (TEMP - cf 6.4.3.2)"
                }
                """.trimIndent()
            )
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
        every { entityService.exists(any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } returns appendResult
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityService.getFullEntityById(any(), any()) } returns mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityEventService.publishAppendEntityAttributesEvents(any(), any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214".toUri())) }
        verify {
            entityService.appendEntityAttributes(
                eq("urn:ngsi-ld:BreedingService:0214".toUri()),
                any(),
                eq(false)
            )
        }
        verify { entityService.getFullEntityById(eq(entityId), any()) }
        verify {
            entityEventService.publishAppendEntityAttributesEvents(
                eq(entityId),
                any(),
                appendResult,
                any(),
                listOf(aquacContext!!)
            )
        }
        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 207 if some attributes could not be appended`() {
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
            listOf(NotUpdatedDetails("wrongAttribute", "overwrite disallowed"))
        )

        every { entityService.exists(any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } returns appendResult
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityService.getFullEntityById(any(), any()) } returns mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityEventService.publishAppendEntityAttributesEvents(any(), any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                {
                    "updated":["$fishNumberAttribute"],
                    "notUpdated":[{"attributeName":"wrongAttribute","reason":"overwrite disallowed"}]
                } 
                """.trimIndent()
            )

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214".toUri())) }
        verify {
            entityService.appendEntityAttributes(
                eq("urn:ngsi-ld:BreedingService:0214".toUri()),
                any(),
                eq(false)
            )
        }
        verify { entityService.getFullEntityById(eq(entityId), any()) }
        verify { entityEventService.publishAppendEntityAttributesEvents(eq(entityId), any(), any(), any(), any()) }
        verify {
            entityEventService.publishAppendEntityAttributesEvents(
                eq(entityId),
                any(),
                appendResult,
                any(),
                listOf(aquacContext!!)
            )
        }

        confirmVerified()
    }

    @Test
    fun `append entity attribute should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        every { entityService.exists(any()) } returns false
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity urn:ngsi-ld:BreedingService:0214 does not exist\"}"
            )

        verify { entityService.exists(eq("urn:ngsi-ld:BreedingService:0214".toUri())) }
        verify { entityEventService wasNot called }

        confirmVerified()
    }

    @Test
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

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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
    fun `partial attribute update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = loadSampleData("aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val deadFish =
            """
                {
                    "id": "$entityId",
                    "type": "DeadFishes",
                    "@context": ["$aquacContext", "$NGSILD_CORE_CONTEXT"]
                }
            """.trimIndent()
        val expectedUpdatedEntity =
            """
                {
                    "id": "$entityId",
                    "type": "DeadFishes"
                }
            """.trimIndent()
        val attrId = "fishNumber"

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityAttributeService.partialUpdateEntityAttribute(any(), any(), any()) } returns true
        every { entityService.getFullEntityById(entityId, true) } returns expandJsonLdEntity(deadFish)
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", aquacHeaderLink)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityService.exists(entityId) }
        verify {
            entityAttributeService.partialUpdateEntityAttribute(eq(entityId), any(), eq(listOf(aquacContext!!)))
        }
        verify { entityService.getFullEntityById(entityId, true) }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as AttributeUpdateEvent
                    it.operationType == EventsType.ATTRIBUTE_UPDATE &&
                        it.entityId == entityId &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == "urn:ngsi-ld:Dataset:1".toUri() &&
                        it.contexts == listOf(aquacContext) &&
                        it.operationPayload.matchContent(jsonLdFile) &&
                        it.updatedEntity.removeNoise() == expectedUpdatedEntity.removeNoise()
                },
                "DeadFishes"
            )
        }
        confirmVerified(entityService, entityEventService)
    }

    @Test
    fun `partial attribute update should return a 404 if entity does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        every { entityService.exists(any()) } returns false

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        verify { entityService.exists(entityId) }
        confirmVerified(entityService)
    }

    @Test
    fun `partial attribute update should return a 404 if attribute does not exist`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityAttributeService.partialUpdateEntityAttribute(any(), any(), any()) } throws
            ResourceNotFoundException("Unknown attribute $attrId in entity $entityId")

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNotFound

        verify { entityService.exists(entityId) }
        verify { authorizationService.userCanUpdateEntity(entityId, "mock-user") }
        verify {
            entityAttributeService.partialUpdateEntityAttribute(eq(entityId), any(), eq(listOf(aquacContext!!)))
        }
        confirmVerified(entityService)
    }

    @Test
    fun `partial attribute update should not authorize user without write rights on entity to update attribute`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_partialAttributeUpdate.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val attrId = "fishNumber"

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns false

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/$attrId")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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

        verify { entityService.exists(entityId) }
        verify { authorizationService.userCanUpdateEntity(entityId, "mock-user") }
        confirmVerified(entityService)
    }

    @Test
    fun `it should not authorize user without write rights on entity to append attributes to it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/BreedingService_newProperty.json")
        val entityId = "urn:ngsi-ld:BreedingService:0214".toUri()

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns false

        webClient.post()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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

    @Test
    fun `entity attributes update should return a 204 if JSON-LD payload is correct`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every {
            entityService.updateEntityAttributes(
                any(),
                any()
            )
        } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.REPLACED
                )
            ),
            notUpdated = arrayListOf()
        )

        every { entityService.getFullEntityById(any(), any()) } returns mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN".toUri())) }
        verify { entityService.updateEntityAttributes(eq(entityId), any()) }
        verify { entityService.getFullEntityById(eq(entityId), any()) }
        verify { entityEventService.publishEntityEvent(any(), any()) }

        confirmVerified(entityService)
    }

    @Test
    fun `entity attributes update should notify for an updated attribute`() {
        val jsonPayload = loadSampleData("aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        every { entityService.exists(any()) } returns true
        every {
            entityService.updateEntityAttributes(
                any(),
                any()
            )
        } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.REPLACED
                )
            ),
            notUpdated = arrayListOf()
        )
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityService.getFullEntityById(any(), any()) } returns JsonLdEntity(
            mapOf(
                "@id" to "urn:ngsi-ld:DeadFishes:019BN",
                "@type" to listOf("DeadFishes")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonPayload)
            .exchange()
            .expectStatus().isNoContent

        verify(timeout = 1000, exactly = 1) {
            entityEventService.publishEntityEvent(
                match {
                    it as AttributeReplaceEvent
                    it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                        it.entityId == entityId &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.operationPayload.matchContent(jsonPayload) &&
                        it.updatedEntity.contains("urn:ngsi-ld:DeadFishes:019BN") &&
                        it.contexts == listOf(aquacContext)
                },
                "DeadFishes"
            )
        }

        confirmVerified(entityEventService)
    }

    @Test
    fun `entity attributes update should send two notification if two attributes are updated`() {
        val fishNumberPayload =
            """
            "fishNumber":{
                "type":"Property",
                "value":600
            }
            """.trimIndent()
        val fishNamePayload =
            """
            "fishName":{
                "type":"Property",
                "datasetId": "urn:ngsi-ld:Dataset:fishName:1",
                "value":"Salmon",
                "unitCode": "C1"
            }
            """.trimIndent()
        val jsonPayload =
            """
            {
                $fishNumberPayload,
                $fishNamePayload
            }
            """.trimIndent()
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()

        every { entityService.exists(any()) } returns true
        every {
            entityService.updateEntityAttributes(
                any(),
                any()
            )
        } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    "https://ontology.eglobalmark.com/aquac#fishName",
                    null,
                    UpdateOperationResult.REPLACED
                ),
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.REPLACED
                )
            ),
            notUpdated = arrayListOf()
        )
        val events = mutableListOf<EntityEvent>()

        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityService.getFullEntityById(any(), any()) } returns JsonLdEntity(
            mapOf(
                "@id" to "urn:ngsi-ld:DeadFishes:019BN",
                "@type" to listOf("DeadFishes")
            ),
            listOf(NGSILD_CORE_CONTEXT)
        )
        every { entityEventService.publishEntityEvent(capture(events), any()) } returns true as java.lang.Boolean

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonPayload)
            .exchange()
            .expectStatus().isNoContent

        verify(timeout = 1000, exactly = 2) { entityEventService.publishEntityEvent(any(), "DeadFishes") }
        events.forEach {
            it as AttributeReplaceEvent
            assertTrue(
                it.operationType == EventsType.ATTRIBUTE_REPLACE &&
                    it.entityId == entityId &&
                    (it.attributeName == "fishName" || it.attributeName == "fishNumber") &&
                    (it.datasetId == null || it.datasetId == "urn:ngsi-ld:Dataset:fishName:1".toUri()) &&
                    (
                        "{$fishNumberPayload}".matchContent(it.operationPayload) ||
                            "{$fishNamePayload}".matchContent(it.operationPayload)
                        ) &&
                    it.updatedEntity.contains("urn:ngsi-ld:DeadFishes:019BN") &&
                    it.contexts == listOf(aquacContext)
            )
        }
    }

    @Test
    fun `entity attributes update should return a 207 if some relationships objects are not found`() {
        val jsonLdFile = ClassPathResource(
            "/ngsild/aquac/fragments/DeadFishes_updateEntityAttributes_invalidAttribute.json"
        )
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val notUpdatedAttribute = NotUpdatedDetails(
            "removedFrom",
            "Property is not valid"
        )
        every { entityService.exists(any()) } returns true
        every {
            entityService.updateEntityAttributes(
                any(),
                any()
            )
        } returns UpdateResult(
            updated = arrayListOf(
                UpdatedDetails(
                    fishNumberAttribute,
                    null,
                    UpdateOperationResult.REPLACED
                )
            ),
            notUpdated = arrayListOf(notUpdatedAttribute)
        )
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityService.getFullEntityById(any(), any()) } returns mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)

        verify { entityService.exists(eq("urn:ngsi-ld:DeadFishes:019BN".toUri())) }
        verify { entityService.updateEntityAttributes(eq(entityId), any()) }
        verify { entityService.getFullEntityById(eq(entityId), any()) }

        confirmVerified(entityService)
    }

    @Test
    fun `entity attributes update should return a 400 if JSON-LD context is not correct`() {
        val payload =
            """
            {
                "name" : "My precious sensor Updated",
                "trigger" : "on"
            }
            """.trimIndent()
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()
        val wrongContext =
            "<http://easyglobalmarket.com/contexts/diat.jsonld>; " +
                "rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", wrongContext)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(payload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "The request includes input data which does not meet the requirements of the operation",
                    "detail": "Unexpected error while parsing payload : loading remote context failed: http://easyglobalmarket.com/contexts/diat.jsonld"
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
                "name" : "My precious sensor Updated",
                "trigger" : "on"
            }
            """.trimIndent()
        val entityId = "urn:ngsi-ld:UnknownType:0022CCC".toUri()

        every { entityService.exists(any()) } returns false

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", aquacHeaderLink)
            .bodyValue(payload)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity urn:ngsi-ld:UnknownType:0022CCC does not exist\"}"
            )

        verify { entityService.exists(eq("urn:ngsi-ld:UnknownType:0022CCC".toUri())) }
    }

    @Test
    fun `it should not authorize user without write rights on entity to update it`() {
        val jsonLdFile = ClassPathResource("/ngsild/aquac/fragments/DeadFishes_updateEntityAttribute.json")
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()

        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns false

        webClient.patch()
            .uri("/ngsi-ld/v1/entities/$entityId/attrs")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
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
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()
        val entity = mockkClass(Entity::class, relaxed = true)
        every { entityService.deleteEntity(any()) } returns Pair(1, 1)
        every { entityService.exists(entityId) } returns true
        every { authorizationService.userIsAdminOfEntity(entityId, "mock-user") } returns true
        every { entityService.getEntityCoreProperties(any()) } returns entity
        every { entity.type } returns listOf("Sensor")
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { entityService.exists(entityId) }
        verify { entityService.deleteEntity(eq(entityId)) }
        verify { entityService.getEntityCoreProperties(eq(entityId)) }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as EntityDeleteEvent
                    it.operationType == EventsType.ENTITY_DELETE &&
                        it.entityId == entityId
                },
                "Sensor"
            )
        }

        confirmVerified(entityService)
    }

    @Test
    fun `delete entity should return a 404 if entity to be deleted has not been found`() {
        val entityId = "urn:ngsi-ld:Sensor:0022CCD".toUri()
        every { entityService.exists(entityId) } returns false

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {"type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"${entityNotFoundMessage("urn:ngsi-ld:Sensor:0022CCD")}"}
                """.trimIndent()
            )

        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete entity should return a 500 if entity could not be deleted`() {
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()
        every { entityService.exists(entityId) } returns true
        every { entityService.getEntityCoreProperties(any()) } returns mockkClass(Entity::class, relaxed = true)
        every { entityService.deleteEntity(any()) } throws RuntimeException("Unexpected server error")
        every { authorizationService.userIsAdminOfEntity(entityId, "mock-user") } returns true

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"Unexpected server error\"}"
            )
    }

    @Test
    fun `it should not authorize user without admin rights on entity to delete it`() {
        val entityId = "urn:ngsi-ld:Sensor:0022CCC".toUri()
        every { entityService.exists(entityId) } returns true
        every { authorizationService.userIsAdminOfEntity(entityId, "mock-user") } returns false

        webClient.delete()
            .uri("/ngsi-ld/v1/entities/$entityId")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden admin access to entity urn:ngsi-ld:Sensor:0022CCC"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete entity attribute should return a 204 if the attribute has been successfully deleted`() {
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updatedEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityService.exists(any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every { entityService.deleteEntityAttributeInstance(any(), any(), any()) } returns true
        every {
            entityService.getFullEntityById(any(), any())
        } returns updatedEntity
        every { updatedEntity.type } returns "DeadFishes"
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { entityService.exists(eq(entityId)) }
        verify {
            entityService.deleteEntityAttributeInstance(
                eq("urn:ngsi-ld:DeadFishes:019BN".toUri()),
                eq("https://ontology.eglobalmark.com/aquac#fishNumber"),
                null
            )
        }
        verify { entityService.getFullEntityById(eq(entityId), any()) }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as AttributeDeleteEvent
                    it.operationType == EventsType.ATTRIBUTE_DELETE &&
                        it.entityId == entityId &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == null &&
                        it.contexts == listOf(aquacContext!!)
                },
                "DeadFishes"
            )
        }
        confirmVerified(entityService)
    }

    @Test
    fun `delete entity attribute should delete all instances if deleteAll flag is true`() {
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updatedEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityService.exists(any()) } returns true
        every { entityService.deleteEntityAttribute(any(), any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every {
            entityService.getFullEntityById(any(), any())
        } returns updatedEntity
        every { updatedEntity.type } returns "DeadFishes"
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/fishNumber?deleteAll=true")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { entityService.exists(eq(entityId)) }
        verify {
            entityService.deleteEntityAttribute(
                eq(entityId),
                eq("https://ontology.eglobalmark.com/aquac#fishNumber")
            )
        }
        verify { entityService.getFullEntityById(eq(entityId), any()) }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as AttributeDeleteAllInstancesEvent
                    it.operationType == EventsType.ATTRIBUTE_DELETE_ALL_INSTANCES &&
                        it.entityId == entityId &&
                        it.attributeName == "fishNumber" &&
                        it.contexts == listOf(aquacContext!!)
                },
                "DeadFishes"
            )
        }
        confirmVerified(entityService)
    }

    @Test
    fun `delete entity attribute should delete instance with the provided datasetId`() {
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        val updatedEntity = mockkClass(JsonLdEntity::class, relaxed = true)
        every { entityService.exists(any()) } returns true
        every { entityService.deleteEntityAttributeInstance(any(), any(), any()) } returns true
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true
        every {
            entityService.getFullEntityById(any(), any())
        } returns updatedEntity
        every { updatedEntity.type } returns "DeadFishes"
        every { entityEventService.publishEntityEvent(any(), any()) } returns true as java.lang.Boolean

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/fishNumber?datasetId=urn:ngsi-ld:Dataset:fishNumber:1")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        verify { entityService.exists(eq(entityId)) }
        verify {
            entityService.deleteEntityAttributeInstance(
                eq(entityId),
                eq("https://ontology.eglobalmark.com/aquac#fishNumber"),
                "urn:ngsi-ld:Dataset:fishNumber:1".toUri()
            )
        }
        verify { entityService.getFullEntityById(eq(entityId), any()) }
        verify {
            entityEventService.publishEntityEvent(
                match {
                    it as AttributeDeleteEvent
                    it.operationType == EventsType.ATTRIBUTE_DELETE &&
                        it.entityId == entityId &&
                        it.attributeName == "fishNumber" &&
                        it.datasetId == "urn:ngsi-ld:Dataset:fishNumber:1".toUri() &&
                        it.contexts == listOf(aquacContext!!)
                },
                "DeadFishes"
            )
        }
        confirmVerified(entityService)
    }

    @Test
    fun `delete entity attribute should return a 404 if the entity is not found`() {
        every { entityService.exists(any()) } returns false

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:DeadFishes:019BN/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Entity urn:ngsi-ld:DeadFishes:019BN does not exist\"}"
            )

        verify { entityEventService wasNot called }
    }

    @Test
    fun `delete entity attribute should return a 404 if the attribute is not found`() {
        every { entityService.exists(any()) } returns true
        every {
            entityService.deleteEntityAttribute(
                any(),
                any()
            )
        } throws ResourceNotFoundException("Attribute Not Found")
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/fishNumber?deleteAll=true")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                    "\"title\":\"The referred resource has not been found\"," +
                    "\"detail\":\"Attribute Not Found\"}"
            )

        verify { entityService.exists(eq(entityId)) }
        verify {
            entityService.deleteEntityAttribute(
                eq("urn:ngsi-ld:DeadFishes:019BN".toUri()),
                eq("https://ontology.eglobalmark.com/aquac#fishNumber")
            )
        }
        verify { entityEventService wasNot called }

        confirmVerified(entityService)
    }

    @Test
    fun `delete entity attribute should return a 500 if the attribute could not be deleted`() {
        every { entityService.exists(any()) } returns true
        every { entityService.deleteEntityAttributeInstance(any(), any(), any()) } returns false
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns true

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
            .expectBody().json(
                "{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/InternalError\"," +
                    "\"title\":\"There has been an error during the operation execution\"," +
                    "\"detail\":\"An error occurred while deleting fishNumber from $entityId\"}"
            )

        verify { entityService.exists(eq(entityId)) }
        verify {
            entityService.deleteEntityAttributeInstance(
                eq("urn:ngsi-ld:DeadFishes:019BN".toUri()),
                eq("https://ontology.eglobalmark.com/aquac#fishNumber"),
                null
            )
        }
        verify { entityEventService wasNot called }

        confirmVerified(entityService)
    }

    @Test
    fun `it should not authorize user without write rights on entity to delete attributes`() {
        every { entityService.exists(any()) } returns true
        val entityId = "urn:ngsi-ld:DeadFishes:019BN".toUri()
        every { authorizationService.userCanUpdateEntity(entityId, "mock-user") } returns false

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/entities/$entityId/attrs/fishNumber")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
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
        verify { entityEventService wasNot called }
    }

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .header("Link", "<$aquacContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isForbidden
    }
}
