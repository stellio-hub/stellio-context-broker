package com.egm.stellio.search.authorization.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.model.EntityAccessRights
import com.egm.stellio.search.authorization.model.User
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.authorization.service.EntityAccessRightsService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.AUTHZ_HEADER_LINK
import com.egm.stellio.shared.util.AUTHZ_TEST_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AUTHZ_TEST_CONTEXT
import com.egm.stellio.shared.util.AccessRight
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS_TERMS
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
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
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

@ActiveProfiles("test")
@WebFluxTest(EntityAccessControlHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class EntityAccessControlHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @Autowired
    private lateinit var applicationProperties: ApplicationProperties

    @MockkBean(relaxed = true)
    private lateinit var entityAccessRightsService: EntityAccessRightsService

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
                "canRead": {
                    "type": "Relationship",
                    "object": "$entityUri1"
                },
                "@context": ["$AUTHZ_TEST_CONTEXT", "$NGSILD_TEST_CORE_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery {
            entityAccessRightsService.setRoleOnEntity(any(), any(), any())
        } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1))

            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri1),
                eq(AccessRight.CAN_READ)
            )
        }
    }

    @Test
    fun `it should allow an authorized user to give access to a set of entities`() {
        val entityUri3 = "urn:ngsi-ld:Entity:entityId3".toUri()
        val requestPayload =
            """
            {
                "canRead": [{
                    "type": "Relationship",
                    "object": "$entityUri1",
                    "datasetId": "$entityUri1"
                },
                {
                    "type": "Relationship",
                    "object": "$entityUri2",
                    "datasetId": "$entityUri2"
                }],
                "canWrite": {
                    "type": "Relationship",
                    "object": "$entityUri3"
                },
                "@context": ["$AUTHZ_TEST_CONTEXT", "$NGSILD_TEST_CORE_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.setRoleOnEntity(any(), any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify(exactly = 3) {
            authorizationService.userCanAdminEntity(
                match { listOf(entityUri1, entityUri2, entityUri3).contains(it) }
            )
        }

        coVerify {
            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri1),
                eq(AccessRight.CAN_READ)
            )
            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri2),
                eq(AccessRight.CAN_READ)
            )
            entityAccessRightsService.setRoleOnEntity(
                eq(otherUserSub),
                eq(entityUri3),
                eq(AccessRight.CAN_WRITE)
            )
        }
    }

    @Test
    fun `it should only allow to give access to authorized entities`() {
        val requestPayload =
            """
            {
                "canRead": [{
                    "type": "Relationship",
                    "object": "$entityUri1",
                    "datasetId": "$entityUri1"
                },
                {
                    "type": "Relationship",
                    "object": "$entityUri2",
                    "datasetId": "$entityUri2"
                }],
                "@context": ["$AUTHZ_TEST_CONTEXT", "$NGSILD_TEST_CORE_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(eq(entityUri1)) } returns Unit.right()
        coEvery {
            authorizationService.userCanAdminEntity(eq(entityUri2))
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
                        "updated":["https://ontology.eglobalmark.com/authorization#canRead"],
                        "notUpdated":[
                          {
                            "attributeName":"https://ontology.eglobalmark.com/authorization#canRead",
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
                eq(AccessRight.CAN_READ)
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
                "@context": ["$AUTHZ_TEST_CONTEXT", "$NGSILD_TEST_CORE_CONTEXT"]
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

        coVerify(exactly = 0) { entityAccessRightsService.setRoleOnEntity(any(), any(), any()) }
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

        coVerify(exactly = 0) { entityAccessRightsService.setRoleOnEntity(any(), any(), any()) }
    }

    @Test
    fun `it should allow an authorized user to remove access to an entity`() {
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.isOwnerOfEntity(any(), any()) } returns false.right()
        coEvery { entityAccessRightsService.removeRoleOnEntity(any(), any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs/$entityUri1")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1))

            entityAccessRightsService.removeRoleOnEntity(eq(otherUserSub), eq(entityUri1))
        }
    }

    @Test
    fun `it should not allow an authorized user to remove access to the owner of an entity`() {
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.isOwnerOfEntity(any(), any()) } returns true.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs/$entityUri1")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "title": "User forbidden to remove ownership of entity",
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied"
}
                """.trimIndent()
            )

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1))
            entityAccessRightsService.isOwnerOfEntity(eq(otherUserSub), eq(entityUri1))
        }
        coVerify(exactly = 0) { entityAccessRightsService.removeRoleOnEntity(eq(otherUserSub), eq(entityUri1)) }
    }

    @Test
    fun `it should not allow an unauthorized user to remove access to an entity`() {
        coEvery {
            authorizationService.userCanAdminEntity(any())
        } returns AccessDeniedException("Access denied").left()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$otherUserSub/attrs/$entityUri1")
            .exchange()
            .expectStatus().isForbidden

        coVerify { authorizationService.userCanAdminEntity(eq(entityUri1)) }
    }

    @Test
    fun `it should return a 404 if the subject has no right on the target entity`() {
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.isOwnerOfEntity(any(), any()) } returns false.right()
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
                    "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title": "No right found for urn:ngsi-ld:User:0123 on urn:ngsi-ld:Entity:entityId1"
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

        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.updateSpecificAccessPolicy(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1))

            entityAccessRightsService.updateSpecificAccessPolicy(
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
                "@context": ["$AUTHZ_TEST_CONTEXT"]
            }
            """.trimIndent()

        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.updateSpecificAccessPolicy(any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1))

            entityAccessRightsService.updateSpecificAccessPolicy(
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

        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.updateSpecificAccessPolicy(any(), any()) } returns Unit.right()

        val expectedAttr = "https://uri.etsi.org/ngsi-ld/default-context/specificAccessPolicy"

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "title": "$expectedAttr is not authorized property name",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData"
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

        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery {
            entityAccessRightsService.updateSpecificAccessPolicy(any(), any())
        } returns BadRequestDataException("Bad request").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().jsonPath("$.title").isEqualTo("Bad request")
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

        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.updateSpecificAccessPolicy(any(), any()) } throws RuntimeException()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().is5xxServerError
    }

    @Test
    fun `it should not allow an unauthorized user to set the specific access policy on an entity`() {
        coEvery {
            authorizationService.userCanAdminEntity(any())
        } returns AccessDeniedException("User is not admin of the target entity").left()

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .bodyValue("{}")
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `it should allow an authorized user to delete the specific access policy on an entity`() {
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { entityAccessRightsService.removeSpecificAccessPolicy(any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            authorizationService.userCanAdminEntity(eq(entityUri1))

            entityAccessRightsService.removeSpecificAccessPolicy(eq(entityUri1))
        }
    }

    @Test
    fun `get authorized entities should return 200 and the number of results if requested limit is 0`() {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(3, emptyList<ExpandedEntity>()).right()

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
        } returns Pair(0, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?limit=1&offset=9")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `get authorized entities should not ask for deleted entities if includeDeleted query param is not provided`() {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(0, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        coVerify {
            authorizationService.getAuthorizedEntities(any(), false, any())
        }
    }

    @Test
    fun `get authorized entities should ask for deleted entities if includeDeleted query param is true`() {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(0, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?includeDeleted=true")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        coVerify {
            authorizationService.getAuthorizedEntities(any(), true, any())
        }
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
                        BEEHIVE_IRI,
                        AccessRight.CAN_READ
                    )
                ),
                createJsonLdEntity(
                    createEntityAccessRight(
                        "urn:ngsi-ld:Beehive:TESTD".toUri(),
                        BEEHIVE_IRI,
                        AccessRight.CAN_ADMIN,
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
                    "type": "$BEEHIVE_IRI",
                    "$AUTH_TERM_RIGHT": {"type":"Property", "value": "canRead"},
                    "@context": "${applicationProperties.contexts.authzCompound}"
                },
                {
                    "id": "urn:ngsi-ld:Beehive:TESTD",
                    "type": "$BEEHIVE_IRI",
                    "$AUTH_TERM_RIGHT": {"type":"Property", "value": "canAdmin"},
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
                    "@context": "${applicationProperties.contexts.authzCompound}"
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
            .jsonPath("$.title")
            .isEqualTo("The attrs parameter only accepts as a value one or more of $ALL_IAM_RIGHTS_TERMS")
    }

    @Test
    fun `get authorized entities should return 204 if authentication is not enabled`() {
        coEvery {
            authorizationService.getAuthorizedEntities(any(), any(), any())
        } returns Pair(-1, emptyList<ExpandedEntity>()).right()

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
        } returns Pair(3, emptyList<ExpandedEntity>()).right()

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
                ExpandedEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:1",
                        "@type" to listOf(GROUP_TYPE),
                        NAME_IRI to buildExpandedPropertyValue("egm")
                    )
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
                        "@context": "${applicationProperties.contexts.authzCompound}"
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
                ExpandedEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:01",
                        "@type" to listOf(GROUP_TYPE),
                        NAME_IRI to buildExpandedPropertyValue("egm"),
                        AuthContextModel.AUTH_REL_IS_MEMBER_OF to buildExpandedPropertyValue("true")
                    )
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups?count=true")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
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
                        "@context": [
                          "$AUTHZ_TEST_CONTEXT",
                          "${applicationProperties.contexts.authzCompound}"
                        ]
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
        } returns Pair(-1, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/groups")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get users should return an access denied error if user is not a stellio admin`() {
        coEvery {
            authorizationService.userIsAdmin()
        } returns AccessDeniedException("Access denied").left()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/users")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `get users should return 200 and the number of results if requested limit is 0`() {
        coEvery { authorizationService.userIsAdmin() } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(3, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/users?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get users should return 204 if authentication is not enabled`() {
        coEvery { authorizationService.userIsAdmin() } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(-1, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/users")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get users should return users if user is a stellio admin`() = runTest {
        coEvery { authorizationService.userIsAdmin() } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(
            1,
            listOf(
                ExpandedEntity(
                    User(
                        "1",
                        USER_TYPE,
                        "username",
                        "givenName",
                        "familyName",
                        mapOf("profile" to "stellio-user", "username" to "username")
                    ).serializeProperties(AUTHZ_TEST_COMPOUND_CONTEXTS)
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
                        "@context": "${applicationProperties.contexts.authzCompound}"
                    }
                ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    private suspend fun createJsonLdEntity(entityAccessRights: EntityAccessRights): ExpandedEntity {
        val earSerialized = entityAccessRights.serializeProperties(AUTHZ_TEST_COMPOUND_CONTEXTS)
        return ExpandedEntity(earSerialized)
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
            canRead = rCanReadUsers
        )

    private fun createSubjectRightInfo(subjectId: URI): List<EntityAccessRights.SubjectRightInfo> {
        val subjectInfo = mapOf(
            AUTH_TERM_KIND to USER_COMPACT_TYPE,
            AUTH_TERM_USERNAME to "stellio-user"
        )
        return listOf(EntityAccessRights.SubjectRightInfo(subjectId, subjectInfo))
    }
}
