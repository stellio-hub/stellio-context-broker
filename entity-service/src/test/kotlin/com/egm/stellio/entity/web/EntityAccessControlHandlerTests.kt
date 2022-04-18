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
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.COMPOUND_AUTHZ_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.NGSILD_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
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

@ActiveProfiles("test")
@WebFluxTest(EntityAccessControlHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", sub = "60AAEBA3-C0C7-42B6-8CB0-0D30857F210E")
class EntityAccessControlHandlerTests {

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
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq(sub)
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
                eq("60AAEBA3-C0C7-42B6-8CB0-0D30857F210E"),
                eq(subjectId),
                any(),
                match {
                    it.updated.size == 1 &&
                        it.updated[0].updateOperationResult == UpdateOperationResult.APPENDED
                },
                eq(listOf(NGSILD_AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT))
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
                "@context": ["$NGSILD_AUTHORIZATION_CONTEXT", "$NGSILD_CORE_CONTEXT"]
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
        every { entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any(), any()) } just Runs

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq(sub)
            )
        }

        verify {
            authorizationService.removeUserRightsOnEntity(eq(entityUri1), eq(subjectId))
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
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns false

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$subjectId/attrs/$entityUri1")
            .exchange()
            .expectStatus().isForbidden

        verify {
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq(sub)
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
                updated = listOf(
                    UpdatedDetails(AUTH_TERM_SAP, entityUri1, UpdateOperationResult.REPLACED)
                )
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
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq(sub)
            )
            entityService.appendEntityAttributes(
                eq(entityUri1),
                match {
                    it.size == 1 &&
                        it[0].name == AUTH_PROP_SAP
                },
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
                    jsonNode["type"].textValue() == "Property" &&
                        jsonNode["value"].textValue() == "AUTH_READ"
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
        every {
            entityEventService.publishAttributeDeleteEvent(any(), any(), any(), any(), any(), any())
        } just Runs

        webClient.delete()
            .uri("/ngsi-ld/v1/entityAccessControl/$entityUri1/attrs/specificAccessPolicy")
            .header(HttpHeaders.LINK, buildContextLinkHeader(NGSILD_AUTHORIZATION_CONTEXT))
            .exchange()
            .expectStatus().isNoContent

        verify {
            authorizationService.userIsAdminOfEntity(
                eq(entityUri1),
                eq(sub)
            )
            entityService.deleteEntityAttribute(
                eq(entityUri1),
                eq(AUTH_PROP_SAP)
            )
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
    fun `get authorized entities should return 200 and the number of results`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getAuthorizedEntities(any(), any(), any(), any(), false)
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
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getAuthorizedEntities(any(), any(), any(), any(), false)
        } returns Pair(0, emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?limit=1&offset=9")
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `get authorized entities should return entities I have rigt on`() {
        every { entityService.exists(any()) } returns true
        every {
            authorizationService.getAuthorizedEntities(any(), any(), any(), any(), false)
        } returns Pair(
            1,
            listOf(
                JsonLdEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:Beehive:TESTC",
                        "@type" to listOf("Beehive")
                    ),
                    listOf(NGSILD_CORE_CONTEXT)
                )
            )
        )

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?limit=1&offset=1&count=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
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
    fun `get authorized entities should return 400 if q parameter is not valid`() {

        webClient.get()
            .uri("/ngsi-ld/v1/entityAccessControl/entities?q=rcanwrite")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody()
            .jsonPath(
                "$.detail",
                "The parameter q only accepts as a value: `rCanRead`, `rCanWrite`, `rCanAdmin`"
            )
    }
}
