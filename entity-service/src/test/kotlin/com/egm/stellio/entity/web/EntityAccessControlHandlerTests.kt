package com.egm.stellio.entity.web

import arrow.core.Some
import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_RIGHT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.COMPOUND_AUTHZ_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.NGSILD_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset

@ActiveProfiles("test")
@WebFluxTest(EntityAccessControlHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", sub = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")
class EntityAccessControlHandlerTests {

    private val authzHeaderLink = buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT)

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    private val sub = Some("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")
    private val subjectId = "urn:ngsi-ld:User:0123".toUri()
    private val entityUri1 = "urn:ngsi-ld:Entity:entityId1".toUri()

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @Test
    fun `it should allow an authorized user to give access to an entity`() {
        val requestPayload =
            """
            {
                "rCanRead": {
                    "type": "Relationship",
                    "object": "$entityUri1"
                },
                "@context": ["$NGSILD_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every {
            entityService.appendEntityRelationship(any(), any(), any(), any())
        } returns UpdateAttributeResult(AUTH_REL_CAN_READ, null, UpdateOperationResult.APPENDED)
        every { entityEventService.publishAttributeAppendEvents(any(), any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub))

            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == AUTH_REL_CAN_READ && it.instances.size == 1 },
                match { it.objectId == entityUri1 },
                eq(false)
            )
            entityEventService.publishAttributeAppendEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(subjectId),
                any(),
                match { it.updated.size == 1 && it.updated[0].updateOperationResult == UpdateOperationResult.APPENDED },
                eq(listOf(NGSILD_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT))
            )
        }
        confirmVerified(authorizationService, entityEventService)
    }

    @Test
    fun `it should allow an authorized user to give access to a set of entities`() {
        val entityUri2 = "urn:ngsi-ld:Entity:entityId2".toUri()
        val entityUri3 = "urn:ngsi-ld:Entity:entityId3".toUri()
        val requestPayload =
            """
            {
                "rCanRead": [{
                    "type": "Relationship",
                    "object": "$entityUri1",
                    "datasetId": "$entityUri1"
                },
                {
                    "type": "Relationship",
                    "object": "$entityUri2",
                    "datasetId": "$entityUri2"
                }],
                "rCanWrite": {
                    "type": "Relationship",
                    "object": "$entityUri3"
                },
                "@context": ["$NGSILD_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        verify(exactly = 3) {
            authorizationService.userIsAdminOfEntity(
                match { listOf(entityUri1, entityUri2, entityUri3).contains(it) },
                eq(sub)
            )
        }

        verify {
            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == AUTH_REL_CAN_READ },
                match { it.objectId == entityUri1 },
                eq(false)
            )
            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == AUTH_REL_CAN_READ },
                match { it.objectId == entityUri2 },
                eq(false)
            )
            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == AUTH_REL_CAN_WRITE },
                match { it.objectId == entityUri3 },
                eq(false)
            )
        }
        confirmVerified(authorizationService, entityService)
    }

    @Test
    fun `it should only allow to give access to authorized entities`() {
        val entityUri2 = "urn:ngsi-ld:Entity:entityId2".toUri()
        val requestPayload =
            """
            {
                "rCanRead": [{
                    "type": "Relationship",
                    "object": "$entityUri1",
                    "datasetId": "$entityUri1"
                },
                {
                    "type": "Relationship",
                    "object": "$entityUri2",
                    "datasetId": "$entityUri2"
                }],
                "@context": ["$NGSILD_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(eq(entityUri1), any()) } returns true
        every { authorizationService.userIsAdminOfEntity(eq(entityUri2), any()) } returns false
        every {
            entityService.appendEntityRelationship(any(), any(), any(), any())
        } returns UpdateAttributeResult(
            attributeName = "rCanRead",
            datasetId = entityUri1,
            updateOperationResult = UpdateOperationResult.APPENDED
        )

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                    {
                        "updated":["rCanRead"],
                        "notUpdated":[
                          {
                            "attributeName":"rCanRead",
                            "reason":"User is not authorized to manage rights on entity urn:ngsi-ld:Entity:entityId2"
                          }
                        ]
                    }
                """
            )

        verify(exactly = 1) {
            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == AUTH_REL_CAN_READ },
                match { it.objectId == entityUri1 },
                eq(false)
            )
        }
        confirmVerified(entityService)
    }

    @Test
    fun `it should filter out invalid attributes when adding rights on entities`() {
        val entityUri2 = "urn:ngsi-ld:Entity:entityId2".toUri()
        val requestPayload =
            """
            {
                "invalidRelationshipName": [{
                    "type": "Relationship",
                    "object": "$entityUri1",
                    "datasetId": "$entityUri1"
                }],
                "aProperty": {
                    "type": "Property",
                    "value": "$entityUri2"
                },
                "@context": ["$NGSILD_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                    {
                        "updated":[],
                        "notUpdated":[
                          {
                            "attributeName":"https://uri.etsi.org/ngsi-ld/default-context/invalidRelationshipName",
                            "reason":"Not a relationship or not an authorized relationship name"
                          },
                          {
                            "attributeName":"https://uri.etsi.org/ngsi-ld/default-context/aProperty",
                            "reason":"Not a relationship or not an authorized relationship name"
                          }
                        ]
                    }
                """
            )

        verify { entityService.appendEntityRelationship(any(), any(), any(), any()) wasNot Called }
        confirmVerified(entityService)
    }

    @Test
    fun `it should not do anything if authorization context is missing when adding rights on entities`() {
        val requestPayload =
            """
            {
                "rCanRead": {
                    "type": "Relationship",
                    "object": "$entityUri1"
                },
                "@context": ["$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                    {
                        "notUpdated":[
                          {
                            "attributeName":"https://uri.etsi.org/ngsi-ld/default-context/rCanRead",
                            "reason":"Not a relationship or not an authorized relationship name"
                          }
                        ]
                    }
                """
            )

        verify {
            entityService.appendEntityRelationship(any(), any(), any(), any()) wasNot Called
        }
        confirmVerified(entityService)
    }

    @Test
    fun `it should allow an authorized user to remove access to an entity`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { authorizationService.removeUserRightsOnEntity(any(), any()) } returns 1
        every { entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any(), any()) } just Runs

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        verify { authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub)) }

        verify { authorizationService.removeUserRightsOnEntity(eq(entityUri1), eq(subjectId)) }

        verify {
            entityEventService.publishAttributeDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(subjectId),
                eq(entityUri1.toString()),
                null,
                eq(false),
                eq(listOf(NGSILD_AUTHORIZATION_CONTEXT))
            )
        }
        confirmVerified(authorizationService, entityEventService)
    }

    @Test
    fun `it should not allow an unauthorized user to remove access to an entity`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns false

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .exchange()
            .expectStatus().isForbidden

        verify { authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub)) }
        confirmVerified(authorizationService)
    }

    @Test
    fun `it should return a 404 if the subject has no right on the target entity`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { authorizationService.removeUserRightsOnEntity(any(), any()) } returns 0

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                    {
                        "detail": "No right found for urn:ngsi-ld:User:0123 on urn:ngsi-ld:Entity:entityId1",
                        "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title": "The referred resource has not been found"
                    }
                """.trimIndent()
            )
    }

    @Test
    fun `it should allow an authorized user to set the specific access policy on an entity`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "AUTH_READ"
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } returns
            UpdateResult(
                notUpdated = emptyList(),
                updated = listOf(UpdatedDetails(AUTH_TERM_SAP, entityUri1, UpdateOperationResult.REPLACED))
            )
        every {
            entityEventService.publishAttributeAppendEvent(any(), any(), any(), any(), any(), any(), any(), any())
        } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub))

            entityService.appendEntityAttributes(
                eq(entityUri1),
                match { it.size == 1 && it[0].name == AUTH_PROP_SAP },
                false
            )
            entityEventService.publishAttributeAppendEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityUri1),
                eq(AUTH_TERM_SAP),
                null,
                eq(true),
                match {
                    val jsonNode = mapper.readTree(it)
                    jsonNode["type"].textValue() == "Property" && jsonNode["value"].textValue() == "AUTH_READ"
                },
                eq(UpdateOperationResult.REPLACED),
                eq(COMPOUND_AUTHZ_CONTEXT)
            )
        }
        confirmVerified(authorizationService, entityService, entityEventService)
    }

    @Test
    fun `it should return a 400 if the payload contains a multi-instance property`() {
        val requestPayload =
            """
            [{
                "type": "Property",
                "value": "AUTH_READ"
            },{
                "type": "Property",
                "value": "AUTH_WRITE"
            }]
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().jsonPath("$.detail", "Payload must only contain a single attribute instance")

        verify { entityService.appendEntityAttributes(any(), any(), any()) wasNot Called }
        confirmVerified(entityService)
    }

    @Test
    fun `it should ignore properties that are not part of the payload when setting a specific access policy`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "datasetId": "urn:ngsi-ld:Dataset:01",
                "value": "AUTH_READ"
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } returns
            UpdateResult(
                notUpdated = emptyList(),
                updated = listOf(UpdatedDetails(AUTH_TERM_SAP, entityUri1, UpdateOperationResult.REPLACED))
            )
        every {
            entityEventService.publishAttributeAppendEvent(any(), any(), any(), any(), any(), any(), any(), any())
        } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `it should return a 400 if the value is not one of the supported`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "someValue"
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().jsonPath("$.detail", "Value must be one of AUTH_READ or AUTH_WRITE")

        verify { entityService.appendEntityAttributes(any(), any(), any()) wasNot Called }
        confirmVerified(entityService)
    }

    @Test
    fun `it should return a 400 if the provided attribute is a relationship`() {
        val requestPayload =
            """
            {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Entity:01"
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().jsonPath("$.detail", "Payload must be a property")

        verify { entityService.appendEntityAttributes(any(), any(), any()) wasNot Called }
        confirmVerified(entityService)
    }

    @Test
    fun `it should return a 500 if the specific access policy could not be persisted`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "AUTH_READ"
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { entityService.appendEntityAttributes(any(), any(), any()) } returns
            UpdateResult(
                notUpdated = listOf(NotUpdatedDetails(AUTH_TERM_SAP, "DB is not available")),
                updated = emptyList()
            )

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().is5xxServerError
    }

    @Test
    fun `it should not allow an unauthorized user to set the specific access policy on an entity`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns false

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue("{}")
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `it should allow an authorized user to delete the specific access policy on an entity`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { entityService.deleteEntityAttribute(any(), any()) } returns true
        every { entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any(), any()) } just Runs

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub))

            entityService.deleteEntityAttribute(eq(entityUri1), eq(AUTH_PROP_SAP))

            entityEventService.publishAttributeDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityUri1),
                eq(AUTH_TERM_SAP),
                null,
                eq(false),
                eq(COMPOUND_AUTHZ_CONTEXT)
            )
        }
        confirmVerified(authorizationService, entityService, entityEventService)
    }

    @Test
    fun `get authorized entities should return 200 and the number of results if requested limit is 0`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getAuthorizedEntities(any(), any(), NGSILD_CORE_CONTEXT)
        } returns Pair(3, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get groups memberships should return 200 and the number of results if requested limit is 0`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getGroupsMemberships(any(), any(), any(), NGSILD_CORE_CONTEXT)
        } returns Pair(3, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get authorized entities should return 200 and empty response if requested offset does not exist`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getAuthorizedEntities(any(), any(), NGSILD_CORE_CONTEXT)
        } returns Pair(0, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?limit=1&offset=9")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `get authorized entities should return entities I have a right on`() {
        val userUri = "urn:ngsi-ld:User:01"
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getAuthorizedEntities(any(), any(), NGSILD_CORE_CONTEXT)
        } returns Pair(
            2,
            listOf(
                createJsonLdEntity(
                    "urn:ngsi-ld:Beehive:TESTC",
                    "Beehive",
                    "urn:ngsi-ld:Dataset:rCanRead:urn:ngsi-ld:Beehive:TESTC",
                    AUTH_TERM_CAN_READ,
                    null,
                    null,
                    null,
                    null,
                    NGSILD_CORE_CONTEXT
                ),
                createJsonLdEntity(
                    "urn:ngsi-ld:Beehive:TESTD",
                    "Beehive",
                    "urn:ngsi-ld:Dataset:rCanAdmin:urn:ngsi-ld:Beehive:TESTD",
                    AUTH_TERM_CAN_ADMIN,
                    AUTH_READ.toString(),
                    userUri.toUri(),
                    null,
                    null,
                    NGSILD_CORE_CONTEXT
                )
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?limit=1&offset=1&count=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "2")
            .expectBody().json(
                """[
                    {
                            "id": "urn:ngsi-ld:Beehive:TESTC",
                            "type": "Beehive",
                            "datasetId": "urn:ngsi-ld:Dataset:rCanRead:urn:ngsi-ld:Beehive:TESTC",
                            "$AUTH_PROP_RIGHT": {"type":"Property", "value": "rCanRead"},
                            "@context": ["$NGSILD_CORE_CONTEXT"]
                        },
                        {
                            "id": "urn:ngsi-ld:Beehive:TESTD",
                            "type": "Beehive",
                            "datasetId": "urn:ngsi-ld:Dataset:rCanAdmin:urn:ngsi-ld:Beehive:TESTD",
                            "$AUTH_PROP_RIGHT": {"type":"Property", "value": "rCanAdmin"},
                            "$AUTH_PROP_SAP": {"type":"Property", "value": "$AUTH_READ"},
                            "$AUTH_REL_CAN_READ": {"type":"Relationship", "object": "$userUri"},
                            "@context": ["$NGSILD_CORE_CONTEXT"]
                        }
                    ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get authorized entities should return entities I have a right on with system attributes`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getAuthorizedEntities(any(), any(), NGSILD_AUTHORIZATION_CONTEXT)
        } returns Pair(
            1,
            listOf(
                createJsonLdEntity(
                    "urn:ngsi-ld:Beehive:TESTC",
                    "Beehive",
                    null,
                    AUTH_TERM_CAN_READ,
                    null,
                    null,
                    "2015-10-18T11:20:30.000001Z",
                    "2015-10-18T11:20:30.000001Z",
                    NGSILD_AUTHORIZATION_CONTEXT
                )
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?options=sysAttrs")
            .header(HttpHeaders.LINK, authzHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody().json(
                """[
                        {
                            "id": "urn:ngsi-ld:Beehive:TESTC",
                            "type": "Beehive",
                            "right": {"type":"Property", "value": "rCanRead"},
                            "createdAt": "2015-10-18T11:20:30.000001Z",
                            "modifiedAt": "2015-10-18T11:20:30.000001Z",
                            "@context": ["$NGSILD_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
                        }
                    ]
                """.trimMargin()
            )
    }

    @Test
    fun `get authorized entities should return 400 if q parameter is not valid`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?q=rcanwrite")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody()
            .jsonPath(
                "$.detail",
                "The parameter q only accepts as a value one or more of ${AuthContextModel.ALL_IAM_RIGHTS_TERMS}"
            )
    }

    @Test
    fun `get authorized entities should return 204 if authentication is not enabled`() {
        every {
            authorizationService.getAuthorizedEntities(any(), any(), NGSILD_CORE_CONTEXT)
        } returns Pair(-1, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get groups memberships should return groups I am member of`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getGroupsMemberships(any(), any(), any(), NGSILD_CORE_CONTEXT)
        } returns Pair(
            1,
            listOf(
                JsonLdEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:1",
                        "@type" to listOf(GROUP_TYPE),
                        NGSILD_NAME_PROPERTY to JsonLdUtils.buildJsonLdExpandedProperty("egm")
                    ),
                    listOf(NGSILD_CORE_CONTEXT)
                )
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups?count=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
            .expectBody().json(
                """[
                        {
                            "id": "urn:ngsi-ld:group:1",
                            "type": "$GROUP_TYPE",
                            "$NGSILD_NAME_PROPERTY" : {"type":"Property", "value": "egm"},
                            "@context": ["$NGSILD_CORE_CONTEXT"]
                        }
                    ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get groups memberships should return groups I am member of with authorization context`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getGroupsMemberships(any(), any(), any(), NGSILD_AUTHORIZATION_CONTEXT)
        } returns Pair(
            1,
            listOf(
                JsonLdEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:01",
                        "@type" to listOf(GROUP_TYPE),
                        NGSILD_NAME_PROPERTY to JsonLdUtils.buildJsonLdExpandedProperty("egm"),
                        AuthContextModel.AUTH_REL_IS_MEMBER_OF to JsonLdUtils.buildJsonLdExpandedProperty("true")
                    ),
                    listOf(NGSILD_AUTHORIZATION_CONTEXT)
                )
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups?count=true")
            .header(HttpHeaders.LINK, authzHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
            .expectBody().json(
                """[
                        {
                            "id": "urn:ngsi-ld:group:01",
                            "type": "Group",
                            "name": {"type":"Property", "value": "egm"},
                            "isMemberOf": {"type":"Property", "value": "true"},
                            "@context": ["$NGSILD_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
                        }
                    ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get groups memberships should return 204 if authentication is not enabled`() {
        every {
            authorizationService.getGroupsMemberships(any(), any(), any(), NGSILD_CORE_CONTEXT)
        } returns Pair(-1, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    private fun createJsonLdEntity(
        id: String,
        type: String,
        datasetId: String? = null,
        right: String,
        specificAccessPolicy: String? = null,
        rCanReadUser: URI? = null,
        createdAt: String? = null,
        modifiedAt: String? = null,
        context: String
    ): JsonLdEntity {
        val jsonLdEntity = mutableMapOf<String, Any>()
        jsonLdEntity[JsonLdUtils.JSONLD_ID] = id
        jsonLdEntity[JsonLdUtils.JSONLD_TYPE] = type
        datasetId?.run {
            jsonLdEntity[JsonLdUtils.NGSILD_DATASET_ID_PROPERTY] = mapOf(
                JsonLdUtils.JSONLD_ID to datasetId
            )
        }
        jsonLdEntity[AUTH_PROP_RIGHT] = JsonLdUtils.buildJsonLdExpandedProperty(right)
        specificAccessPolicy?.run {
            jsonLdEntity[AUTH_PROP_SAP] = JsonLdUtils.buildJsonLdExpandedProperty(specificAccessPolicy)
        }
        rCanReadUser?.run {
            jsonLdEntity[AUTH_REL_CAN_READ] = listOf(
                JsonLdUtils.buildJsonLdExpandedRelationship(rCanReadUser)
            )
        }
        createdAt?.run {
            jsonLdEntity[NGSILD_CREATED_AT_PROPERTY] =
                JsonLdUtils.buildJsonLdExpandedDateTime(Instant.parse(createdAt).atZone(ZoneOffset.UTC))
        }
        modifiedAt?.run {
            jsonLdEntity[NGSILD_MODIFIED_AT_PROPERTY] =
                JsonLdUtils.buildJsonLdExpandedDateTime(Instant.parse(createdAt).atZone(ZoneOffset.UTC))
        }
        return JsonLdEntity(jsonLdEntity, listOf(context))
    }
}
