package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.UpdateAttributeResult
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.service.EntityEventService
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.NGSILD_EGM_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.verify
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@WebFluxTest(EntityAccessControlHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", username = "mock-user")
class EntityAccessControlHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @MockkBean(relaxed = true)
    private lateinit var entityEventService: EntityEventService

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
                "@context": ["$NGSILD_EGM_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every {
            entityService.appendEntityRelationship(any(), any(), any(), any())
        } returns UpdateAttributeResult(AUTH_REL_CAN_READ, null, UpdateOperationResult.APPENDED)
        every { entityEventService.publishAttributeAppendEvents(any(), any(), any(), any()) } just Runs

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq("mock-user")
            )
        }

        verify {
            entityService.appendEntityRelationship(
                eq(subjectId),
                match {
                    it.name == AUTH_REL_CAN_READ &&
                        it.instances.size == 1
                },
                match { it.objectId == entityUri1 },
                eq(false)
            )
        }

        verify {
            entityEventService.publishAttributeAppendEvents(
                eq(subjectId),
                any(),
                match {
                    it.updated.size == 1 &&
                        it.updated[0].updateOperationResult == UpdateOperationResult.APPENDED
                },
                eq(listOf(NGSILD_EGM_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT))
            )
        }
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
                "@context": ["$NGSILD_EGM_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq("mock-user")
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
                "@context": ["$NGSILD_EGM_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
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
                "@context": ["$NGSILD_EGM_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
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
                            "attributeName":"invalidRelationshipName",
                            "reason":"Not a relationship or not an authorized relationship name"
                          },
                          {
                            "attributeName":"aProperty",
                            "reason":"Not a relationship or not an authorized relationship name"
                          }
                        ]
                    }
                """
            )

        verify { entityService.appendEntityRelationship(any(), any(), any(), any()) wasNot Called }
    }

    @Test
    fun `it should allow an authorized user to remove access to an entity`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { authorizationService.removeUserRightsOnEntity(any(), any()) } returns 1
        every { entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any()) } just Runs

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_EGM_AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq("mock-user")
            )
        }

        verify {
            authorizationService.removeUserRightsOnEntity(eq(entityUri1), eq(subjectId))
        }

        verify {
            entityEventService.publishAttributeDeleteEvent(
                eq(subjectId),
                eq(entityUri1.toString()),
                null,
                eq(false),
                eq(listOf(NGSILD_EGM_AUTHORIZATION_CONTEXT))
            )
        }
    }

    @Test
    fun `it should not allow an unauthorized user to remove access to an entity`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns false

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .exchange()
            .expectStatus().isForbidden

        verify {
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq("mock-user")
            )
        }
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
}
