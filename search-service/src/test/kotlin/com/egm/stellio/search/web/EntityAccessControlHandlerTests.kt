package com.egm.stellio.search.web

import arrow.core.Some
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.*
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.*
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
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @MockkBean(relaxed = true)
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

    private val sub = Some("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")
    private val subjectId = "urn:ngsi-ld:User:0123".toUri()
    private val entityUri1 = "urn:ngsi-ld:Entity:entityId1".toUri()
    private val entityUri2 = "urn:ngsi-ld:Entity:entityId2".toUri()

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

        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAccessRightsService.setRoleOnEntity(any(), any(), any())
        } returns Unit.right()
        every { entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), true, any()) } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub))

            entityAccessRightsService.setRoleOnEntity(
                eq(sub.value),
                match { it == entityUri1 },
                match { it.name == AUTH_REL_CAN_READ }
            )
        }

        verify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(subjectId),
                any(),
                match { it.updated.size == 1 && it.updated[0].updateOperationResult == UpdateOperationResult.APPENDED },
                true,
                eq(listOf(NGSILD_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT))
            )
        }
    }

    @Test
    fun `it should allow an authorized user to give access to a set of entities`() {
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

        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify(exactly = 3) {
            authorizationService.userIsAdminOfEntity(
                match { listOf(entityUri1, entityUri2, entityUri3).contains(it) },
                eq(sub)
            )

            entityAccessRightsService.setRoleOnEntity(
                eq(sub.value),
                match { it == entityUri1 },
                match { it.name == AUTH_REL_CAN_READ }
            )
            entityAccessRightsService.setRoleOnEntity(
                eq(sub.value),
                match { it == entityUri2 },
                match { it.name == AUTH_REL_CAN_READ }
            )
            entityAccessRightsService.setRoleOnEntity(
                eq(sub.value),
                match { it == entityUri3 },
                match { it.name == AUTH_REL_CAN_WRITE }
            )
        }
    }

    @Test
    fun `it should only allow to give access to authorized entities`() {
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

        coEvery { authorizationService.userIsAdminOfEntity(eq(entityUri1), any()) } returns Unit.right()
        coEvery {
            authorizationService.userIsAdminOfEntity(eq(entityUri2), any())
        } returns AccessDeniedException("Access denied").left()
        coEvery { entityAccessRightsService.setRoleOnEntity(any(), any(), any()) } returns Unit.right()

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

        coVerify(exactly = 1) {
            entityAccessRightsService.setRoleOnEntity(
                eq(sub.value),
                match { it == entityUri1 },
                match { it.name == AUTH_REL_CAN_READ }
            )
        }
    }

    @Test
    fun `it should filter out invalid attributes when adding rights on entities`() {
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

        coVerify { entityAccessRightsService.setRoleOnEntity(any(), any(), any()) wasNot Called }
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

        coVerify {
            entityAccessRightsService.setRoleOnEntity(any(), any(), any()) wasNot Called
        }
    }

    @Test
    fun `it should allow an authorized user to remove access to an entity`() {
        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()
        coEvery { entityAccessRightsService.removeRoleOnEntity(any(), any()) } returns Unit.right()
        every { entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any(), any()) } just Runs

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub))

            entityAccessRightsService.removeRoleOnEntity(eq(sub.value), eq(entityUri1))
        }

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
    }

    @Test
    fun `it should not allow an unauthorized user to remove access to an entity`() {
        coEvery {
            authorizationService.userIsAdminOfEntity(any(), any())
        } returns AccessDeniedException("Access denied").left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .exchange()
            .expectStatus().isForbidden

        coVerify { authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub)) }
    }

    @Test
    fun `it should return a 404 if the subject has no right on the target entity`() {
        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAccessRightsService.removeRoleOnEntity(any(), any())
        } returns ResourceNotFoundException("No right found for $subjectId on $entityUri1").left()

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

        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.updateSpecificAccessPolicy(any(), any()) } returns Unit.right()
        every {
            entityEventService.publishAttributeChangeEvents(any(), any(), any(), any(), any(), any())
        } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub))

            entityPayloadService.updateSpecificAccessPolicy(
                eq(entityUri1),
                match {
                    it.getAttributeInstances().size == 1 &&
                        it.name == AUTH_PROP_SAP &&
                        it.getAttributeInstances()[0] is NgsiLdPropertyInstance &&
                        (it.getAttributeInstances()[0] as NgsiLdPropertyInstance).value.toString() == "AUTH_READ"
                }
            )
        }

        verify {
            entityEventService.publishAttributeChangeEvents(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityUri1),
                match {
                    it.containsKey(AUTH_PROP_SAP)
                },
                match {
                    it.updated.size == 1 &&
                        it.updated[0].updateOperationResult == UpdateOperationResult.UPDATED &&
                        it.updated[0].attributeName == AUTH_TERM_SAP &&
                        it.updated[0].datasetId == null
                },
                eq(true),
                eq(COMPOUND_AUTHZ_CONTEXT)
            )
        }
    }

    @Test
    fun `it should return a 400 if the payload to set specific access policy is not correct`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "someValue"
            }
            """.trimIndent()

        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.updateSpecificAccessPolicy(any(), any())
        } returns BadRequestDataException("Bad request").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().jsonPath("$.detail", "Bad request")
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

        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.updateSpecificAccessPolicy(any(), any()) } throws RuntimeException()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().is5xxServerError
    }

    @Test
    fun `it should not allow an unauthorized user to set the specific access policy on an entity`() {
        coEvery {
            authorizationService.userIsAdminOfEntity(any(), any())
        } returns AccessDeniedException("User is not admin of the target entity").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .bodyValue("{}")
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `it should allow an authorized user to delete the specific access policy on an entity`() {
        coEvery { authorizationService.userIsAdminOfEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.removeSpecificAccessPolicy(any()) } returns Unit.right()
        every { entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any(), any()) } just Runs

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userIsAdminOfEntity(eq(entityUri1), eq(sub))

            entityPayloadService.removeSpecificAccessPolicy(eq(entityUri1))
        }

        verify {
            entityEventService.publishAttributeDeleteEvent(
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(entityUri1),
                eq(AUTH_TERM_SAP),
                null,
                eq(false),
                eq(COMPOUND_AUTHZ_CONTEXT)
            )
        }
    }

    @Test
    fun `get authorized entities should return 200 and the number of results if requested limit is 0`() {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), NGSILD_CORE_CONTEXT, any())
        } returns Pair(3, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get authorized entities should return 200 and empty response if requested offset does not exist`() {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), NGSILD_CORE_CONTEXT, any())
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
        coEvery {
            authorizationService.getAuthorizedEntities(any(), NGSILD_CORE_CONTEXT, any())
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
        coEvery {
            authorizationService.getAuthorizedEntities(any(), NGSILD_AUTHORIZATION_CONTEXT, any())
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
        coEvery {
            authorizationService.getAuthorizedEntities(any(), NGSILD_CORE_CONTEXT, any())
        } returns Pair(-1, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get groups memberships should return 200 and the number of results if requested limit is 0`() {
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any())
        } returns Pair(3, emptyList<JsonLdEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get groups memberships should return groups I am member of`() {
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any())
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
        ).right()

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
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any())
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
        ).right()

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
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any())
        } returns Pair(-1, emptyList<JsonLdEntity>()).right()

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
