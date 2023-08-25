package com.egm.stellio.search.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.authorization.EntityAccessRights
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.authorization.User
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_FAMILY_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_GIVEN_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_KIND
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_RIGHT
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SUBJECT_INFO
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_USERNAME
import com.egm.stellio.shared.util.AuthContextModel.GROUP_COMPACT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.AuthContextModel.USER_COMPACT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedProperty
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.net.URI
import java.time.Duration

@OptIn(ExperimentalCoroutinesApi::class)
@ActiveProfiles("test")
@WebFluxTest(EntityAccessControlHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class EntityAccessControlHandlerTests {

    private val authzHeaderLink = buildContextLinkHeader(AUTHORIZATION_CONTEXT)

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @MockkBean(relaxed = true)
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    private val otherUserSub = "D8916880-D0DC-4469-82F7-F294AEF7292D"
    private val subjectId = "urn:ngsi-ld:User:0123".toUri()
    private val entityUri1 = "urn:ngsi-ld:Entity:entityId1".toUri()
    private val entityUri2 = "urn:ngsi-ld:Entity:entityId2".toUri()

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .responseTimeout(Duration.ofMinutes(5))
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
                "@context": ["$AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAccessRightsService.setRoleOnEntity(any(), any(), any())
        } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1), eq(sub))

            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri1),
                eq(AccessRight.R_CAN_READ)
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
                "@context": ["$AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityAccessRightsService.setRoleOnEntity(any(), any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify(exactly = 3) {
            authorizationService.userCanAdminEntity(
                match { listOf(entityUri1, entityUri2, entityUri3).contains(it) },
                eq(sub)
            )
        }

        coVerify {
            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri1),
                eq(AccessRight.R_CAN_READ)
            )
            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri2),
                eq(AccessRight.R_CAN_READ)
            )
            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri3),
                eq(AccessRight.R_CAN_WRITE)
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
                "@context": ["$AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(eq(entityUri1), any()) } returns Unit.right()
        coEvery {
            authorizationService.userCanAdminEntity(eq(entityUri2), any())
        } returns AccessDeniedException("Access denied").left()
        coEvery { entityAccessRightsService.setRoleOnEntity(any(), any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                    {
                        "updated":["https://ontology.eglobalmark.com/authorization#rCanRead"],
                        "notUpdated":[
                          {
                            "attributeName":"https://ontology.eglobalmark.com/authorization#rCanRead",
                            "reason":"User is not authorized to manage rights on entity urn:ngsi-ld:Entity:entityId2"
                          }
                        ]
                    }
                """
            )

        coVerify(exactly = 1) {
            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri1),
                eq(AccessRight.R_CAN_READ)
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
                "@context": ["$AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs")
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
                "@context": ["$APIC_COMPOUND_CONTEXT"]
            }
            """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs")
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
        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityAccessRightsService.removeRoleOnEntity(any(), any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs/$entityUri1")
            .header(HttpHeaders.LINK, buildContextLinkHeader(AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1), eq(sub))

            entityAccessRightsService.removeRoleOnEntity(eq(otherUserSub), eq(entityUri1))
        }
    }

    @Test
    fun `it should not allow an unauthorized user to remove access to an entity`() {
        coEvery {
            authorizationService.userCanAdminEntity(any(), any())
        } returns AccessDeniedException("Access denied").left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs/$entityUri1")
            .exchange()
            .expectStatus().isForbidden

        coVerify { authorizationService.userCanAdminEntity(eq(entityUri1), eq(sub)) }
    }

    @Test
    fun `it should return a 404 if the subject has no right on the target entity`() {
        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityAccessRightsService.removeRoleOnEntity(any(), any())
        } returns ResourceNotFoundException("No right found for $subjectId on $entityUri1").left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs/$entityUri1")
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

        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.updateSpecificAccessPolicy(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(AUTHORIZATION_CONTEXT))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1), eq(sub))

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
    }

    @Test
    fun `it should allow an authorized user to set the specific access policy on an entity with context in payload`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "AUTH_READ",
                "@context": ["$AUTHORIZATION_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.updateSpecificAccessPolicy(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1), eq(sub))

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
    }

    @Test
    fun `it should not do anything if authz context is missing when setting the specific access policy on an entity`() {
        val requestPayload =
            """
            {
                "type": "Property",
                "value": "AUTH_READ",
                "@context": ["$APIC_COMPOUND_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.updateSpecificAccessPolicy(any(), any()) } returns Unit.right()

        val expectedAttr = "https://uri.etsi.org/ngsi-ld/default-context/specificAccessPolicy"

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "detail": "$expectedAttr is not authorized property name",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation"
                }
                """.trimIndent()
            )
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

        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery {
            entityPayloadService.updateSpecificAccessPolicy(any(), any())
        } returns BadRequestDataException("Bad request").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(AUTHORIZATION_CONTEXT))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
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

        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.updateSpecificAccessPolicy(any(), any()) } throws RuntimeException()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(AUTHORIZATION_CONTEXT))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().is5xxServerError
    }

    @Test
    fun `it should not allow an unauthorized user to set the specific access policy on an entity`() {
        coEvery {
            authorizationService.userCanAdminEntity(any(), any())
        } returns AccessDeniedException("User is not admin of the target entity").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(AUTHORIZATION_CONTEXT))
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue("{}")
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `it should allow an authorized user to delete the specific access policy on an entity`() {
        coEvery { authorizationService.userCanAdminEntity(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.removeSpecificAccessPolicy(any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1), eq(sub))

            entityPayloadService.removeSpecificAccessPolicy(eq(entityUri1))
        }
    }

    @Test
    fun `get authorized entities should return 200 and the number of results if requested limit is 0`() {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(3, emptyList<JsonLdEntity>()).right()

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
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(0, emptyList<JsonLdEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?limit=1&offset=9")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `get authorized entities should return entities I have a right on`() = runTest {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(
            2,
            listOf(
                createJsonLdEntity(
                    createEntityAccessRight(
                        "urn:ngsi-ld:Beehive:TESTC".toUri(),
                        BEEHIVE_TYPE,
                        AccessRight.R_CAN_READ
                    )
                ),
                createJsonLdEntity(
                    createEntityAccessRight(
                        "urn:ngsi-ld:Beehive:TESTD".toUri(),
                        BEEHIVE_TYPE,
                        AccessRight.R_CAN_ADMIN,
                        AUTH_READ,
                        createSubjectRightInfo(subjectId)
                    )
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?limit=1&offset=1&count=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "2")
            .expectBody().json(
                """
                [{
                    "id": "urn:ngsi-ld:Beehive:TESTC",
                    "type": "$BEEHIVE_TYPE",
                    "$AUTH_TERM_RIGHT": {"type":"Property", "value": "rCanRead"},
                    "@context": ["$AUTHORIZATION_COMPOUND_CONTEXT"]
                },
                {
                    "id": "urn:ngsi-ld:Beehive:TESTD",
                    "type": "$BEEHIVE_TYPE",
                    "$AUTH_TERM_RIGHT": {"type":"Property", "value": "rCanAdmin"},
                    "$AUTH_TERM_SAP": {"type":"Property", "value": "$AUTH_READ"},
                    "$AUTH_TERM_CAN_READ": {
                        "type":"Relationship",
                         "datasetId": "urn:ngsi-ld:Dataset:0123",
                         "object": "$subjectId",
                         "$AUTH_TERM_SUBJECT_INFO": {
                            "type":"Property", 
                            "value": {"$AUTH_TERM_KIND": "User", "$AUTH_TERM_USERNAME": "stellio-user"}
                         }
                    },
                    "@context": ["$AUTHORIZATION_COMPOUND_CONTEXT"]
                }]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get authorized entities should return 400 if attrs parameter is not valid`() {
        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?attrs=rcanwrite")
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
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(-1, emptyList<JsonLdEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get groups memberships should return 200 and the number of results if requested limit is 0`() {
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
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
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
        } returns Pair(
            1,
            listOf(
                JsonLdEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:1",
                        "@type" to listOf(GROUP_TYPE),
                        NGSILD_NAME_PROPERTY to buildExpandedProperty("egm")
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
                """
                [
                    {
                        "id": "urn:ngsi-ld:group:1",
                        "type": "$GROUP_COMPACT_TYPE",
                        "name" : {"type":"Property", "value": "egm"},
                        "@context": ["$AUTHORIZATION_COMPOUND_CONTEXT"]
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
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
        } returns Pair(
            1,
            listOf(
                JsonLdEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:01",
                        "@type" to listOf(GROUP_TYPE),
                        NGSILD_NAME_PROPERTY to buildExpandedProperty("egm"),
                        AuthContextModel.AUTH_REL_IS_MEMBER_OF to buildExpandedProperty("true")
                    ),
                    listOf(AUTHORIZATION_CONTEXT)
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
                """
                [
                    {
                        "id": "urn:ngsi-ld:group:01",
                        "type": "Group",
                        "name": {"type":"Property", "value": "egm"},
                        "isMemberOf": {"type":"Property", "value": "true"},
                        "@context": ["$AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
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
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
        } returns Pair(-1, emptyList<JsonLdEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get users should return an access denied error if user is not a stellio admin`() {
        coEvery {
            authorizationService.userIsAdmin(any())
        } returns AccessDeniedException("Access denied").left()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/users")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `get users should return 200 and the number of results if requested limit is 0`() {
        coEvery { authorizationService.userIsAdmin(any()) } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(3, emptyList<JsonLdEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/users?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get users should return 204 if authentication is not enabled`() {
        coEvery { authorizationService.userIsAdmin(any()) } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(-1, emptyList<JsonLdEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/users")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get users should return users if user is a stellio admin`() = runTest {
        coEvery { authorizationService.userIsAdmin(any()) } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(
            1,
            listOf(
                JsonLdEntity(
                    User(
                        "1",
                        USER_TYPE,
                        "username",
                        "givenName",
                        "familyName",
                        mapOf("profile" to "stellio-user", "username" to "username")
                    ).serializeProperties(AUTHORIZATION_COMPOUND_CONTEXT),
                    listOf(NGSILD_CORE_CONTEXT)
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/users?count=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
            .expectBody().json(
                """
                [
                    {
                        "id": "urn:ngsi-ld:User:1",
                        "type": "$USER_COMPACT_TYPE",
                        "$AUTH_TERM_USERNAME" : { "type":"Property", "value": "username" },
                        "$AUTH_TERM_GIVEN_NAME" : { "type":"Property", "value": "givenName" },
                        "$AUTH_TERM_FAMILY_NAME" : { "type":"Property", "value": "familyName" },
                        "$AUTH_TERM_SUBJECT_INFO": { "type":"Property","value":{ "profile": "stellio-user" } },
                        "@context": ["$AUTHORIZATION_COMPOUND_CONTEXT"]
                    }
                ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    private suspend fun createJsonLdEntity(
        entityAccessRights: EntityAccessRights,
        context: String = NGSILD_CORE_CONTEXT
    ): JsonLdEntity {
        val earSerialized = entityAccessRights.serializeProperties(AUTHORIZATION_COMPOUND_CONTEXT)
        return JsonLdEntity(earSerialized, listOf(context))
    }

    private fun createEntityAccessRight(
        id: URI,
        type: ExpandedTerm,
        accessRight: AccessRight,
        specificAccessPolicy: AuthContextModel.SpecificAccessPolicy? = null,
        rCanReadUsers: List<EntityAccessRights.SubjectRightInfo>? = null
    ): EntityAccessRights =
        EntityAccessRights(
            id = id,
            types = listOf(type),
            right = accessRight,
            specificAccessPolicy = specificAccessPolicy,
            rCanReadUsers = rCanReadUsers
        )

    private fun createSubjectRightInfo(subjectId: URI): List<EntityAccessRights.SubjectRightInfo> {
        val subjectInfo = mapOf(
            AUTH_TERM_KIND to USER_COMPACT_TYPE,
            AUTH_TERM_USERNAME to "stellio-user"
        )
        return listOf(EntityAccessRights.SubjectRightInfo(subjectId, subjectInfo))
    }
}
