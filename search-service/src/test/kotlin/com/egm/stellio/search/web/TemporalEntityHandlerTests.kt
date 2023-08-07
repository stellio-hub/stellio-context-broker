package com.egm.stellio.search.web

import arrow.core.Either
import arrow.core.Some
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
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
import org.springframework.web.reactive.function.BodyInserters
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class TemporalEntityHandlerTests {

    private lateinit var apicHeaderLink: String

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var queryService: QueryService

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val temporalEntityAttributeName = "speed"
    private val attributeInstanceId = "urn:ngsi-ld:Instance:01".toUri()

    @BeforeAll
    fun configureWebClientDefaults() {
        apicHeaderLink = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    private fun buildDefaultMockResponsesForAddAttributes() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
    }

    @Test
    fun `create temporal entity should return a 201`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/temporal/beehive_create_temporal_entity.jsonld")

        coEvery { entityPayloadService.checkEntityExistence(any(), any()) } returns Unit.right()
        coEvery { authorizationService.userCanCreateEntities(sub) } returns Unit.right()
        coEvery { entityPayloadService.createEntity(any<NgsiLdEntity>(), any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.upsertAttributes(any(), any(), any()) } returns Unit.right()
        coEvery { authorizationService.createAdminRight(any(), any()) } returns Unit.right()

        val data =
            loadSampleData("/temporal/beehive_create_temporal_entity_first_instance.jsonld").deserializeAsMap()
        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(data, listOf(APIC_COMPOUND_CONTEXT))
        val expectedInstancesFilePath =
            "/temporal/beehive_create_temporal_entity_without_first_instance_expanded.jsonld"
        val jsonInstances =
            loadSampleData(expectedInstancesFilePath).deserializeAsMap() as ExpandedAttributes

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().value("Location", Is.`is`("/ngsi-ld/v1/temporal/entities/$entityUri"))

        coVerify { authorizationService.userCanCreateEntities(eq(sub)) }
        coVerify { entityPayloadService.checkEntityExistence(eq(entityUri), true) }
        coVerify {
            entityPayloadService.createEntity(
                any(),
                eq(jsonLdEntity),
                eq(sub.value)
            )
        }
        coVerify {
            entityPayloadService.upsertAttributes(
                eq(entityUri),
                eq(jsonInstances),
                eq(sub.value)
            )
        }
        coVerify { authorizationService.createAdminRight(eq(entityUri), eq(sub)) }
    }

    @Test
    fun `update temporal entity should return a 204`() {
        val jsonLdFile = ClassPathResource("/ngsild/temporal/beehive_update_temporal_entity.jsonld")

        coEvery { authorizationService.userCanUpdateEntity(entityUri, sub) } returns Unit.right()
        coEvery {
            entityPayloadService.checkEntityExistence(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage("urn:ngsi-ld:BeeHive:TESTC")).left()
        coEvery { entityPayloadService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        val expectedInstancesFilePath =
            "/temporal/beehive_update_temporal_entity_without_mandatory_fields_expanded.jsonld"
        val jsonInstances =
            loadSampleData(expectedInstancesFilePath).deserializeAsMap() as ExpandedAttributes

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify { authorizationService.userCanUpdateEntity(eq(entityUri), eq(sub)) }
        coVerify {
            entityPayloadService.upsertAttributes(
                eq(entityUri),
                eq(jsonInstances),
                eq(sub.value)
            )
        }
    }

    @Test
    fun `it should correctly handle an attribute with one instance`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_one_attribute_one_instance.jsonld")

        buildDefaultMockResponsesForAddAttributes()
        coEvery { entityPayloadService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 1
                },
                any()
            )
        }
    }

    @Test
    fun `it should correctly handle an attribute with many instances`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_one_attribute_many_instances.jsonld")

        buildDefaultMockResponsesForAddAttributes()
        coEvery { entityPayloadService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 1
                },
                any()
            )
        }
    }

    @Test
    fun `it should correctly handle many attributes with one instance`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_one_instance.jsonld")

        buildDefaultMockResponsesForAddAttributes()
        coEvery { entityPayloadService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 2
                },
                any()
            )
        }
    }

    @Test
    fun `it should correctly handle many attributes with many instances`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_many_instances.jsonld")

        buildDefaultMockResponsesForAddAttributes()
        coEvery { entityPayloadService.upsertAttributes(any(), any(), any()) } returns Unit.right()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        coVerify {
            entityPayloadService.upsertAttributes(
                eq(entityUri),
                match {
                    it.size == 2
                },
                any()
            )
        }
    }

    @Test
    fun `it should return a 400 if temporal entity fragment is badly formed`() {
        buildDefaultMockResponsesForAddAttributes()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue("{ \"id\": \"bad\" }"))
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Unable to expand input payload"
                }
                """
            )
    }

    @Test
    fun `it should return a 403 is user is not authorized to write on the entity`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_many_instances.jsonld")

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isForbidden

        coVerify { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) wasNot Called }
        coVerify { attributeInstanceService.addAttributeInstance(any(), any(), any()) wasNot Called }
    }

    private fun buildDefaultMockResponsesForGetEntity() {
        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery { authorizationService.userCanReadEntity(any(), any()) } returns Unit.right()
    }

    @Test
    fun `it should raise a 400 if timerel is present without time query param`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'timerel' and 'time' must be used in conjunction"
                }
                """
            )
    }

    @Test
    fun `it should raise a 400 if time is present without timerel query param`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timeAt=2020-10-29T18:00:00Z")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'timerel' and 'time' must be used in conjunction"
                }
                """
            )
    }

    @Test
    fun `it should give a 200 if no timerel and no time query params are in the request`() {
        buildDefaultMockResponsesForGetEntity()

        coEvery {
            queryService.queryTemporalEntity(any(), any(), any())
        } returns emptyMap<String, Any>().right()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isOk

        coVerify {
            authorizationService.userCanReadEntity(eq(entityUri), eq(Some(MOCK_USER_SUB)))
        }
    }

    @Test
    fun `it should raise a 400 if timerel is between and no endTimeAt provided`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=between&timeAt=startTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'endTimeAt' request parameter is mandatory if 'timerel' is 'between'"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if time is not parsable`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=before&timeAt=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'timeAt' parameter is not a valid date"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if timerel is not a valid value`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?timerel=befor&timeAt=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'timerel' is not valid, it should be one of 'before', 'between', or 'after'"
                }
                """
            )
    }

    @Test
    fun `it should raise a 400 if timerel is between and endTimeAt is not parseable`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=endTime"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'endTimeAt' parameter is not a valid date"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if one of time bucket or aggregate is missing`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&aggrPeriodDuration=P1DT1H&options=aggregatedValues"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'aggrMethods' is mandatory if 'aggregatedValues' option is specified"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if aggregate function is unknown`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&options=aggregatedValues&aggrMethods=unknown"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'unknown' is not a recognized aggregation method for 'aggrMethods' parameter"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if aggrPeriodDuration is not in the correct format`() {
        buildDefaultMockResponsesForGetEntity()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&options=aggregatedValues&aggrPeriodDuration=PXD3N"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'aggrMethods' is mandatory if 'aggregatedValues' option is specified"
                } 
                """
            )
    }

    @Test
    fun `it should return a 404 if temporal entity attribute does not exist`() {
        buildDefaultMockResponsesForGetEntity()

        coEvery {
            queryService.queryTemporalEntity(any(), any(), any())
        } throws ResourceNotFoundException("Entity urn:ngsi-ld:BeeHive:TESTC was not found")

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"${entityNotFoundMessage(entityUri.toString())}"
                }
                """
            )
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid`() {
        buildDefaultMockResponsesForGetEntity()

        coEvery {
            queryService.queryTemporalEntity(any(), any(), any())
        } returns emptyMap<String, Any>().right()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify {
            queryService.queryTemporalEntity(
                eq(entityUri),
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        ) &&
                        !temporalEntitiesQuery.withTemporalValues &&
                        !temporalEntitiesQuery.withAudit
                },
                eq(APIC_COMPOUND_CONTEXT)
            )
        }
        confirmVerified(queryService)
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid and entity is publicly readable`() {
        buildDefaultMockResponsesForGetEntity()

        coEvery {
            queryService.queryTemporalEntity(any(), any(), any())
        } returns emptyMap<String, Any>().right()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify { authorizationService.userCanReadEntity(eq(entityUri), any()) }
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid and user can read the entity`() {
        buildDefaultMockResponsesForGetEntity()

        coEvery {
            queryService.queryTemporalEntity(any(), any(), any())
        } returns emptyMap<String, Any>().right()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify {
            authorizationService.userCanReadEntity(eq(entityUri), eq(Some(MOCK_USER_SUB)))
        }
    }

    @Test
    fun `it should return an entity with two temporal properties evolution`() {
        buildDefaultMockResponsesForGetEntity()

        mockWithIncomingAndOutgoingTemporalProperties(false)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$.incoming.length()").isEqualTo(2)
            .jsonPath("$.outgoing.length()").isEqualTo(2)
            .jsonPath("$.@context").exists()
    }

    @Test
    fun `it should return a json entity with two temporal properties evolution`() {
        buildDefaultMockResponsesForGetEntity()

        mockWithIncomingAndOutgoingTemporalProperties(false)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .header("Accept", MediaType.APPLICATION_JSON.toString())
            .exchange()
            .expectStatus().isOk
            .expectHeader().exists("Link")
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$.incoming.length()").isEqualTo(2)
            .jsonPath("$.outgoing.length()").isEqualTo(2)
            .jsonPath("$.@context").doesNotExist()
    }

    @Test
    fun `it should return an entity with two temporal properties evolution with temporalValues option`() {
        buildDefaultMockResponsesForGetEntity()

        mockWithIncomingAndOutgoingTemporalProperties(true)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&options=temporalValues"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$..observedAt").doesNotExist()
            .jsonPath("$.incoming[0].values.length()").isEqualTo(2)
            .jsonPath("$.outgoing[0].values.length()").isEqualTo(2)
    }

    private fun mockWithIncomingAndOutgoingTemporalProperties(withTemporalValues: Boolean) {
        val entityTemporalProperties = listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            .map {
                TemporalEntityAttribute(
                    entityId = entityUri,
                    attributeName = it,
                    attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
                    createdAt = ZonedDateTime.now(ZoneOffset.UTC),
                    payload = EMPTY_JSON_PAYLOAD
                )
            }
        val entityFileName = if (withTemporalValues)
            "beehive_with_two_temporal_attributes_evolution_temporal_values.jsonld"
        else
            "beehive_with_two_temporal_attributes_evolution.jsonld"

        val entityWith2temporalEvolutions = deserializeObject(loadSampleData(entityFileName))
        coEvery {
            temporalEntityAttributeService.getForEntity(any(), any())
        } returns listOf(entityTemporalProperties[0], entityTemporalProperties[1])

        val attributes = listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
        val values = listOf(Pair(1543, "2020-01-24T13:01:22.066Z"), Pair(1600, "2020-01-24T14:01:22.066Z"))
        val attInstanceResults = attributes.flatMap {
            values.map {
                SimplifiedAttributeInstanceResult(
                    temporalEntityAttribute = UUID.randomUUID(),
                    value = it.first,
                    time = ZonedDateTime.parse(it.second)
                )
            }
        }
        listOf(Pair(0, entityTemporalProperties[0]), Pair(2, entityTemporalProperties[1])).forEach {
            coEvery {
                attributeInstanceService.search(any(), it.second)
            } returns listOf(attInstanceResults[it.first], attInstanceResults[it.first + 1]).right()
        }

        coEvery {
            queryService.queryTemporalEntity(any(), any(), any())
        } returns entityWith2temporalEvolutions.right()
    }

    @Test
    fun `it should raise a 400 and return an error response`() {
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities?type=Beehive&timerel=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'timerel' and 'time' must be used in conjunction"
                }
                """
            )
    }

    @Test
    fun `it should return a 200 with empty payload if no temporal attribute is found`() {
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(emptyList(), 2))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&type=BeeHive"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        coVerify {
            queryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.queryParams.limit == 30 &&
                        temporalEntitiesQuery.queryParams.offset == 0 &&
                        temporalEntitiesQuery.queryParams.ids.isEmpty() &&
                        temporalEntitiesQuery.queryParams.type == BEEHIVE_TYPE &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        !temporalEntitiesQuery.withTemporalValues
                },
                any()
            )
        }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should return 200 with jsonld response body for query temporal entities`() {
        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$.length()").isEqualTo(2)
            .jsonPath("$[0].@context").exists()
            .jsonPath("$[1].@context").exists()
    }

    @Test
    fun `it should return 200 with json response body for query temporal entities`() {
        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive"
            )
            .header("Accept", MediaType.APPLICATION_JSON.toString())
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectHeader().exists("Link")
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$.length()").isEqualTo(2)
            .jsonPath("$[0].@context").doesNotExist()
            .jsonPath("$[1].@context").doesNotExist()
    }

    @Test
    fun `query temporal entity should return 200 with prev link header if exists`() {
        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=2"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                "</ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=1>;rel=\"prev\";type=\"application/ld+json\""
            )
    }

    @Test
    fun `query temporal entity should return 200 and empty response if requested offset does not exist`() {
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery { queryService.queryTemporalEntities(any(), any()) } returns Either.Right(Pair(emptyList(), 2))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=9"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }

    @Test
    fun `query temporal entities should return 200 and the number of results if count is asked for`() {
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery { queryService.queryTemporalEntities(any(), any()) } returns Either.Right(Pair(emptyList(), 2))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=0&offset=9&count=true"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "2")
            .expectBody()
    }

    @Test
    fun `query temporal entity should return 200 with next link header if exists`() {
        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=0"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                "</ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=1>;rel=\"next\";type=\"application/ld+json\""
            )
    }

    @Test
    fun `query temporal entity should return 200 with prev and next link header if exists`() {
        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 3))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=1"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader()
            .valueEquals(
                "Link",
                "</ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=0>;rel=\"prev\";type=\"application/ld+json\"",
                "</ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=2>;rel=\"next\";type=\"application/ld+json\""
            )
    }

    @Test
    fun `query temporal entity should return 400 if requested offset is less than zero`() {
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } throws BadRequestDataException(
            "Offset must be greater than zero and limit must be strictly greater than zero"
        )

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=1&offset=-1"
            )
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
    fun `query temporal entity should return 400 if limit is equal or less than zero`() {
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } throws BadRequestDataException(
            "Offset must be greater than zero and limit must be strictly greater than zero"
        )

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=-1&offset=1"
            )
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
    fun `query temporal entity should return 400 if limit is greater than the maximum authorized limit`() {
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } throws BadRequestDataException(
            "You asked for 200 results, but the supported maximum limit is 100"
        )

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "type=BeeHive&limit=200&offset=1"
            )
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
    fun `modify attribute instance should return a 204 if an instance has been successfully modified`() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery { attributeInstanceService.modifyAttributeInstance(any(), any(), any(), any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY/$attributeInstanceId")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify { entityPayloadService.checkEntityExistence(entityUri) }
        coVerify { authorizationService.userCanUpdateEntity(entityUri, sub) }
        coVerify {
            attributeInstanceService.modifyAttributeInstance(
                entityUri,
                TEMPERATURE_PROPERTY,
                attributeInstanceId,
                any()
            )
        }
    }

    @Test
    fun `modify attribute instance should return a 403 is user is not authorized to update an entity `() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), sub)
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY/$attributeInstanceId")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden write access to entity $entityUri"
                }
                """.trimIndent()
            )

        coVerify { entityPayloadService.checkEntityExistence(entityUri) }
        coVerify { authorizationService.userCanUpdateEntity(entityUri, sub) }
    }

    @Test
    fun `modify attribute instance should return a 404 if entity to be update has not been found`() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")

        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY/$attributeInstanceId")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"Entity urn:ngsi-ld:BeeHive:TESTC was not found"
                }
                """.trimIndent()
            )
        coVerify { entityPayloadService.checkEntityExistence(entityUri) }
    }

    @Test
    fun `modify attribute instance should return a 404 if attributeInstanceId or attribute name is not found`() {
        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, NGSILD_CORE_CONTEXT)

        coEvery { entityPayloadService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            attributeInstanceService.modifyAttributeInstance(any(), any(), any(), any())
        } returns ResourceNotFoundException(
            attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())
        ).left()

        webClient.patch()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY/$attributeInstanceId")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(instanceTemporalFragment))
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "detail":"${attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found"
                }
                """.trimIndent()
            )

        coVerify { entityPayloadService.checkEntityExistence(entityUri) }
        coVerify { authorizationService.userCanUpdateEntity(entityUri, sub) }
    }

    @Test
    fun `delete temporal entity should return a 204 if an entity has been successfully deleted`() {
        coEvery { entityPayloadService.checkEntityExistence(entityUri) } returns Unit.right()
        coEvery { authorizationService.userCanAdminEntity(entityUri, sub) } returns Unit.right()
        coEvery { entityPayloadService.deleteEntity(any()) } returns Unit.right()
        coEvery { authorizationService.removeRightsOnEntity(any()) } returns Unit.right()

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityPayloadService.checkEntityExistence(entityUri)
            authorizationService.userCanAdminEntity(eq(entityUri), eq(sub))
            entityPayloadService.deleteEntity(eq(entityUri))
            authorizationService.removeRightsOnEntity(eq(entityUri))
        }
    }

    @Test
    fun `delete temporal entity should return a 404 if entity to be deleted has not been found`() {
        coEvery {
            entityPayloadService.checkEntityExistence(entityUri)
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"${entityNotFoundMessage(entityUri.toString())}"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete temporal entity should return a 400 if the provided id is not a valid URI`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/beehive")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"The supplied identifier was expected to be an URI but it is not: beehive"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete temporal entity should return a 400 if entity id is missing`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Missing entity id when trying to delete temporal entity"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete temporal entity should return a 500 if entity could not be deleted`() {
        coEvery { entityPayloadService.checkEntityExistence(entityUri) } returns Unit.right()
        coEvery { authorizationService.userCanAdminEntity(entityUri, sub) } returns Unit.right()
        coEvery {
            entityPayloadService.deleteEntity(any())
        } throws RuntimeException("Unexpected server error")

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
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
    fun `delete temporal entity should return a 403 is user is not authorized to delete an entity`() {
        coEvery { entityPayloadService.checkEntityExistence(entityUri) } returns Unit.right()
        coEvery {
            authorizationService.userCanAdminEntity(entityUri, sub)
        } returns AccessDeniedException("User forbidden admin access to entity $entityUri").left()

        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden admin access to entity $entityUri"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 204 if the attribute has been successfully deleted`() {
        coEvery { temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(eq(entityUri), eq(TEMPERATURE_PROPERTY))
            authorizationService.userCanUpdateEntity(eq(entityUri), eq(sub))
            entityPayloadService.deleteAttribute(
                eq(entityUri),
                eq(TEMPERATURE_PROPERTY),
                null
            )
        }
    }

    @Test
    fun `delete attribute temporal should delete all instances if deleteAll flag is true`() {
        coEvery { temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityPayloadService.deleteAttribute(
                eq(entityUri),
                eq(TEMPERATURE_PROPERTY),
                null,
                eq(true)
            )
        }
    }

    @Test
    fun `delete attribute temporal should delete instance with the provided datasetId`() {
        val datasetId = "urn:ngsi-ld:Dataset:temperature:1"
        coEvery { temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any(), any())
        } returns Unit.right()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any())
        } returns Unit.right()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY?datasetId=$datasetId")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify {
            entityPayloadService.deleteAttribute(
                eq(entityUri),
                eq(TEMPERATURE_PROPERTY),
                eq(datasetId.toUri())
            )
        }
    }

    @Test
    fun `delete attribute temporal should return a 404 if the entity is not found`() {
        coEvery {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"Entity urn:ngsi-ld:BeeHive:TESTC was not found"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 404 if the attribute is not found`() {
        coEvery { temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any(), any())
        } throws ResourceNotFoundException("Attribute Not Found")

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY?deleteAll=true")
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .contentType(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"Attribute Not Found"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 400 if the request is not correct`() {
        coEvery { temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any(), sub) } returns Unit.right()
        coEvery {
            entityPayloadService.deleteAttribute(any(), any(), any())
        } returns BadRequestDataException("Something is wrong with the request").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY")
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
    }

    @Test
    fun `delete attribute temporal should return a 400 if the provided id is not a valid URI`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/beehive/attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"The supplied identifier was expected to be an URI but it is not: beehive"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 400 if entity id is missing`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities//attrs/$TEMPERATURE_COMPACT_PROPERTY")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Missing entity id or attribute id when trying to delete an attribute temporal"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 400 if attribute name is missing`() {
        webClient.delete()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Missing entity id or attribute id when trying to delete an attribute temporal"
                }
                """.trimIndent()
            )
    }

    @Test
    fun `delete attribute temporal should return a 403 if user is not allowed to update entity`() {
        coEvery { temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any()) } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), sub)
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient.method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$TEMPERATURE_COMPACT_PROPERTY")
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
    }

    @Test
    fun `delete attribute instance temporal should return 204`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, NGSILD_CORE_CONTEXT)
        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            attributeInstanceService.deleteInstance(any(), any(), any())
        } returns Unit.right()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$temporalEntityAttributeName/$attributeInstanceId")
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify { entityPayloadService.checkEntityExistence(entityUri) }
        coVerify {
            attributeInstanceService.deleteInstance(entityUri, expandedAttr, attributeInstanceId)
        }
        coVerify {
            authorizationService.userCanUpdateEntity(eq(entityUri), eq(Some(MOCK_USER_SUB)))
        }
    }

    @Test
    fun `delete attribute instance temporal should return 404 if entityId is not found`() {
        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$temporalEntityAttributeName/$attributeInstanceId")
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "detail":"${entityNotFoundMessage(entityUri.toString())}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found"
                }
                """.trimIndent()
            )

        coVerify {
            entityPayloadService.checkEntityExistence(entityUri)
        }
        confirmVerified(entityPayloadService)
    }

    @Test
    fun `delete attribute instance temporal should return 404 if attributeInstanceId or attribute name is not found`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, NGSILD_CORE_CONTEXT)

        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery { authorizationService.userCanUpdateEntity(any(), any()) } returns Unit.right()
        coEvery {
            attributeInstanceService.deleteInstance(any(), any(), any())
        } returns ResourceNotFoundException(
            attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())
        ).left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$temporalEntityAttributeName/$attributeInstanceId")
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "detail":"${attributeOrInstanceNotFoundMessage(expandedAttr, attributeInstanceId.toString())}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found"
                }
                """.trimIndent()
            )

        coVerify { entityPayloadService.checkEntityExistence(entityUri) }
        coVerify {
            attributeInstanceService.deleteInstance(entityUri, expandedAttr, attributeInstanceId)
        }
        coVerify {
            authorizationService.userCanUpdateEntity(
                eq(entityUri),
                eq(Some(MOCK_USER_SUB))
            )
        }
    }

    @Test
    fun `delete attribute instance temporal should return 403 if user is not allowed`() {
        coEvery {
            entityPayloadService.checkEntityExistence(any())
        } returns Unit.right()
        coEvery { entityPayloadService.getTypes(any()) } returns listOf(BEEHIVE_TYPE).right()
        coEvery {
            authorizationService.userCanUpdateEntity(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity $entityUri").left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$temporalEntityAttributeName/$attributeInstanceId")
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isForbidden
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/AccessDenied",
                    "title": "The request tried to access an unauthorized resource",
                    "detail": "User forbidden write access to entity $entityUri"
                }
                """.trimIndent()
            )

        coVerify { entityPayloadService.checkEntityExistence(entityUri) }
        coVerify {
            authorizationService.userCanUpdateEntity(
                eq(entityUri),
                eq(Some(MOCK_USER_SUB))
            )
        }
    }

    @Test
    fun `query temporal entity should return 400 if neither type nor attrs is present`() {
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Either type or attrs need to be present in request parameters"
                }
                """.trimIndent()
            )
    }
}
