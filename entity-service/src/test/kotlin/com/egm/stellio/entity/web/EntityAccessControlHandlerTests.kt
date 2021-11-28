package com.egm.stellio.entity.web

import com.egm.stellio.entity.authorization.AuthorizationService
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_READ
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_WRITE
import com.egm.stellio.entity.config.WebSecurityTestConfig
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.service.EntityService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.EventsType
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.concurrent.SettableListenableFuture

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
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    private val subjectId = "urn:ngsi-ld:User:0123".toUri()
    private val entityUri1 = "urn:ngsi-ld:Entity:entityId1".toUri()
    private val entityType = "https://my.context/Entity"

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
        val entity = mockk<Entity>()
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
        every { entityService.getEntityCoreProperties(any()) } returns entity
        every { entity.type } returns listOf(entityType)
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

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
                    it.name == R_CAN_READ &&
                        it.instances.size == 1
                },
                match { it.objectId == entityUri1 },
                eq(false)
            )
        }

        verify {
            kafkaTemplate.send(
                eq("cim.iam.rights"),
                eq(subjectId.toString()),
                match {
                    val event = JsonUtils.deserializeAs<AttributeAppendEvent>(it)
                    val expectedOperationPayload =
                        "{\"type\":\"Relationship\",\"object\":\"urn:ngsi-ld:Entity:entityId1\"}"
                    event.entityId == subjectId &&
                        event.attributeName == "rCanRead" &&
                        event.operationType == EventsType.ATTRIBUTE_APPEND &&
                        event.contexts.size == 2 &&
                        event.datasetId == null &&
                        event.operationPayload.matchContent(expectedOperationPayload)
                }
            )
        }
    }

    @Test
    fun `it should allow an authorized user to give access to a set of entities`() {
        val entity = mockk<Entity>()
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
        every { entityService.getEntityCoreProperties(any()) } returns entity
        every { entity.type } returns listOf(entityType)

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
                match { it.name == R_CAN_READ },
                match { it.objectId == entityUri1 },
                eq(false)
            )
            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == R_CAN_READ },
                match { it.objectId == entityUri2 },
                eq(false)
            )
            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == R_CAN_WRITE },
                match { it.objectId == entityUri3 },
                eq(false)
            )
        }
    }

    @Test
    fun `it should only allow to give access to authorized entities`() {
        val entity = mockk<Entity>()
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
        every { entityService.getEntityCoreProperties(any()) } returns entity
        every { entity.type } returns listOf(entityType)

        webClient.post()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs")
            .bodyValue(requestPayload)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.MULTI_STATUS)
            .expectBody().json(
                """
                    { "unauthorized entities": ["urn:ngsi-ld:Entity:entityId2"]}
                """
            )

        verify(exactly = 1) {
            entityService.appendEntityRelationship(
                eq(subjectId),
                match { it.name == R_CAN_READ },
                match { it.objectId == entityUri1 },
                eq(false)
            )
        }
    }

    @Test
    fun `it should allow an authorized user to remove access to an entity`() {
        val entity = mockk<Entity>()

        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns true
        every { authorizationService.removeUserRightsOnEntity(any(), any()) } returns 1
        every { entityService.getEntityCoreProperties(any()) } returns entity
        every { entity.type } returns listOf(entityType)
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

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
            kafkaTemplate.send(
                eq("cim.iam.rights"),
                eq(subjectId.toString()),
                match {
                    val event = JsonUtils.deserializeAs<AttributeDeleteEvent>(it)
                    event.entityId == subjectId &&
                        event.attributeName == entityUri1.toString() &&
                        event.operationType == EventsType.ATTRIBUTE_DELETE &&
                        event.contexts.size == 1
                }
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
                        "detail": "Subject urn:ngsi-ld:User:0123 has no right on entity urn:ngsi-ld:Entity:entityId1",
                        "type": "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                        "title": "The referred resource has not been found"
                    }
                """.trimIndent()
            )
    }
}
