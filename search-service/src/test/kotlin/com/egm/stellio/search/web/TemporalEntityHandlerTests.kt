package com.egm.stellio.search.web

import arrow.core.Some
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityAccessRightsService
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.util.parseAndCheckQueryParams
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.reactive.function.BodyInserters
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.UUID

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", sub = "0768A6D5-D87B-4209-9A22-8C40A8961A79")
class TemporalEntityHandlerTests {

    private lateinit var apicHeaderLink: String

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var queryService: QueryService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val temporalEntityAttributeName = "speed"
    private val attributeInstanceId = "urn:ngsi-ld:Instance:01".toUri()

    @BeforeAll
    fun configureWebClientDefaults() {
        apicHeaderLink = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()

        mockkStatic(::parseAndCheckQueryParams)
    }

    @AfterAll
    fun clearMock() {
        unmockkStatic(::parseAndCheckQueryParams)
    }

    @Test
    fun `it should correctly handle an attribute with one instance`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_one_attribute_one_instance.jsonld")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery { entityAccessRightsService.canWriteEntity(any(), any()) } answers { Unit.right() }
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } answers {
            Mono.just(temporalEntityAttributeUuid)
        }
        every { attributeInstanceService.addAttributeInstance(any(), any(), any(), any()) } answers {
            Mono.just(1)
        }

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(entityUri),
                eq(INCOMING_PROPERTY)
            )
        }
        verify {
            attributeInstanceService.addAttributeInstance(
                eq(temporalEntityAttributeUuid),
                eq("incoming"),
                match {
                    it.size == 4
                },
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
        confirmVerified(temporalEntityAttributeService, attributeInstanceService)
    }

    @Test
    fun `it should correctly handle an attribute with many instances`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_one_attribute_many_instances.jsonld")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery { entityAccessRightsService.canWriteEntity(any(), any()) } answers { Unit.right() }
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } answers {
            Mono.just(temporalEntityAttributeUuid)
        }
        every { attributeInstanceService.addAttributeInstance(any(), any(), any(), any()) } answers {
            Mono.just(1)
        }

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(entityUri),
                eq(INCOMING_PROPERTY)
            )
        }
        verify(exactly = 2) {
            attributeInstanceService.addAttributeInstance(
                eq(temporalEntityAttributeUuid),
                eq("incoming"),
                match {
                    it.size == 4
                },
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
        confirmVerified(temporalEntityAttributeService, attributeInstanceService)
    }

    @Test
    fun `it should correctly handle many attributes with one instance`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_one_instance.jsonld")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery { entityAccessRightsService.canWriteEntity(any(), any()) } answers { Unit.right() }
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } answers {
            Mono.just(temporalEntityAttributeUuid)
        }
        every { attributeInstanceService.addAttributeInstance(any(), any(), any(), any()) } answers {
            Mono.just(1)
        }

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        verify(exactly = 2) {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(entityUri),
                or(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            )
        }
        verify(exactly = 2) {
            attributeInstanceService.addAttributeInstance(
                eq(temporalEntityAttributeUuid),
                or("incoming", "outgoing"),
                match {
                    it.size == 4
                },
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
        confirmVerified(temporalEntityAttributeService, attributeInstanceService)
    }

    @Test
    fun `it should correctly handle many attributes with many instances`() {
        val entityTemporalFragment =
            loadSampleData("fragments/temporal_entity_fragment_many_attributes_many_instances.jsonld")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        coEvery { entityAccessRightsService.canWriteEntity(any(), any()) } answers { Unit.right() }
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } answers {
            Mono.just(temporalEntityAttributeUuid)
        }
        every { attributeInstanceService.addAttributeInstance(any(), any(), any(), any()) } answers {
            Mono.just(1)
        }

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(entityTemporalFragment))
            .exchange()
            .expectStatus().isNoContent

        verify(exactly = 4) {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(entityUri),
                or(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            )
        }
        verify(exactly = 4) {
            attributeInstanceService.addAttributeInstance(
                eq(temporalEntityAttributeUuid),
                or("incoming", "outgoing"),
                match {
                    it.size == 4
                },
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
        confirmVerified(temporalEntityAttributeService, attributeInstanceService)
    }

    @Test
    fun `it should return a 400 if temporal entity fragment is badly formed`() {
        coEvery { entityAccessRightsService.canWriteEntity(any(), any()) } answers { Unit.right() }

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
        coEvery {
            entityAccessRightsService.canWriteEntity(any(), any())
        } answers { AccessDeniedException("User forbidden write access to entity $entityUri").left() }

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", buildContextLinkHeader(APIC_COMPOUND_CONTEXT))
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(""))
            .exchange()
            .expectStatus().isForbidden

        verify { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) wasNot Called }
        verify { attributeInstanceService.addAttributeInstance(any(), any(), any(), any()) wasNot Called }
        confirmVerified(temporalEntityAttributeService, attributeInstanceService)
    }

    @Test
    fun `it should raise a 400 if timerel is present without time query param`() {
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

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
        coEvery { queryService.queryTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isOk

        coVerify {
            entityAccessRightsService.canReadEntity(
                eq(Some("0768A6D5-D87B-4209-9A22-8C40A8961A79")),
                eq(entityUri)
            )
        }
    }

    @Test
    fun `it should raise a 400 if timerel is between and no endTimeAt provided`() {
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&timeBucket=1 minute"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'timeBucket' and 'aggregate' must be used in conjunction"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if aggregate function is unknown`() {
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Entity:01?" +
                    "timerel=after&timeAt=2020-01-31T07:31:39Z&timeBucket=1 minute&aggregate=unknown"
            )
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Value 'unknown' is not supported for 'aggregate' parameter"
                } 
                """
            )
    }

    @Test
    fun `it should return a 404 if temporal entity attribute does not exist`() {
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
        coEvery {
            queryService.queryTemporalEntity(any(), any(), any(), any(), any())
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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
        coEvery { queryService.queryTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

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
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.timeAt!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                withTemporalValues = false,
                withAudit = false,
                eq(APIC_COMPOUND_CONTEXT)
            )
        }
        confirmVerified(queryService)
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid and entity is publicly readable`() {
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
        coEvery { queryService.queryTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify {
            entityAccessRightsService.canReadEntity(
                any(),
                eq(entityUri)
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid and user can read the entity`() {
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
        coEvery { queryService.queryTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify {
            entityAccessRightsService.canReadEntity(
                eq(Some("0768A6D5-D87B-4209-9A22-8C40A8961A79")),
                eq(entityUri)
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should return an entity with two temporal properties evolution`() {
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
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
        coEvery { entityAccessRightsService.canReadEntity(any(), any()) } answers { Unit.right() }
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
                    type = BEEHIVE_COMPACT_TYPE,
                    attributeName = it,
                    attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                )
            }
        val entityFileName = if (withTemporalValues)
            "beehive_with_two_temporal_attributes_evolution_temporal_values.jsonld"
        else
            "beehive_with_two_temporal_attributes_evolution.jsonld"

        val entityWith2temporalEvolutions = deserializeObject(loadSampleData(entityFileName))
        every {
            temporalEntityAttributeService.getForEntity(any(), any())
        } answers {
            Flux.just(entityTemporalProperties[0], entityTemporalProperties[1])
        }

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
            every {
                attributeInstanceService.search(any(), it.second, withTemporalValues)
            } returns Mono.just(listOf(attInstanceResults[it.first], attInstanceResults[it.first + 1]))
        }

        coEvery {
            queryService.queryTemporalEntity(any(), any(), any(), any(), any())
        } returns entityWith2temporalEvolutions
    }

    @Test
    fun `it should raise a 400 and return an error response`() {
        every {
            parseAndCheckQueryParams(any(), any(), any())
        } throws BadRequestDataException("'timerel' and 'time' must be used in conjunction")
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities?timerel=before")
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

        every { parseAndCheckQueryParams(any(), any(), any()) } returns
            buildDefaultQueryParams().copy(
                queryParams = QueryParams(type = BEEHIVE_COMPACT_TYPE, offset = 0, limit = 20),
                temporalQuery = temporalQuery
            )
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
        } returns Pair(emptyList(), 2)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&type=BeeHive"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        verify {
            parseAndCheckQueryParams(
                any(),
                match {
                    it.size == 4 &&
                        it.getFirst("timerel") == "between" &&
                        it.getFirst("timeAt") == "2019-10-17T07:31:39Z" &&
                        it.getFirst("endTimeAt") == "2019-10-18T07:31:39Z" &&
                        it.getFirst("type") == "BeeHive"
                },
                APIC_COMPOUND_CONTEXT
            )
        }
        coVerify {
            queryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.queryParams.limit == 30 &&
                        temporalEntitiesQuery.queryParams.offset == 0 &&
                        temporalEntitiesQuery.queryParams.id != null &&
                        temporalEntitiesQuery.queryParams.type == "BeeHive" &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        !temporalEntitiesQuery.withTemporalValues
                },
                eq(APIC_COMPOUND_CONTEXT),
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

        every { parseAndCheckQueryParams(any(), any(), any()) } returns buildDefaultQueryParams()
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
        } returns Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2)

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

        every { parseAndCheckQueryParams(any(), any(), any()) } returns buildDefaultQueryParams()
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
        } returns Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2)

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
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Sensor:0022CCC")
            .exchange()
            .expectStatus().isUnauthorized
    }

    @Test
    fun `query temporal entity should return 200 with prev link header if exists`() {

        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")

        every {
            parseAndCheckQueryParams(any(), any(), any())
        } returns buildDefaultQueryParams().copy(queryParams = QueryParams(limit = 1, offset = 2))
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
        } returns Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2)

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
        every { parseAndCheckQueryParams(any(), any(), any()) } returns buildDefaultQueryParams()
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery { queryService.queryTemporalEntities(any(), any(), any()) } returns Pair(emptyList(), 2)

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
        every {
            parseAndCheckQueryParams(any(), any(), any())
        } returns buildDefaultQueryParams().copy(queryParams = QueryParams(limit = 0, offset = 30, count = true))
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery { queryService.queryTemporalEntities(any(), any(), any()) } returns Pair(emptyList(), 2)

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

        every {
            parseAndCheckQueryParams(any(), any(), any())
        } returns buildDefaultQueryParams().copy(queryParams = QueryParams(limit = 1, offset = 0))
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
        } returns Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2)

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

        every {
            parseAndCheckQueryParams(any(), any(), any())
        } returns buildDefaultQueryParams().copy(queryParams = QueryParams(limit = 1, offset = 1))
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
        } returns Pair(listOf(firstTemporalEntity, secondTemporalEntity), 3)

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
        every { parseAndCheckQueryParams(any(), any(), any()) } returns buildDefaultQueryParams()
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
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
        every { parseAndCheckQueryParams(any(), any(), any()) } returns buildDefaultQueryParams()
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
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
        every { parseAndCheckQueryParams(any(), any(), any()) } returns buildDefaultQueryParams()
        coEvery { entityAccessRightsService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any(), any())
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
    fun `delete attribute instance temporal should return 204`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, JsonLdUtils.NGSILD_CORE_CONTEXT)!!
        coEvery { entityAccessRightsService.canWriteEntity(any(), any()) } answers { Unit.right() }
        coEvery {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any())
        } returns Unit.right()
        coEvery {
            attributeInstanceService.deleteEntityAttributeInstance(any(), any(), any())
        } returns Unit.right()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$temporalEntityAttributeName/$attributeInstanceId")
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isNoContent
            .expectBody().isEmpty

        coVerify { temporalEntityAttributeService.checkEntityAndAttributeExistence(entityUri, expandedAttr) }
        coVerify {
            attributeInstanceService.deleteEntityAttributeInstance(entityUri, expandedAttr, attributeInstanceId)
        }
        coVerify {
            entityAccessRightsService.canWriteEntity(
                eq(Some("0768A6D5-D87B-4209-9A22-8C40A8961A79")),
                eq(entityUri)
            )
        }
        confirmVerified(temporalEntityAttributeService, attributeInstanceService, entityAccessRightsService)
    }

    @Test
    fun `delete attribute instance temporal should return 404 if entityId is not found`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, JsonLdUtils.NGSILD_CORE_CONTEXT)!!

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.empty() }
        coEvery {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any())
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
            temporalEntityAttributeService.checkEntityAndAttributeExistence(entityUri, expandedAttr)
        }
        confirmVerified(temporalEntityAttributeService)
    }

    @Test
    fun `delete attribute instance temporal should return 404 if temporalEntityAttributeName is not found`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, JsonLdUtils.NGSILD_CORE_CONTEXT)!!

        every {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any())
        } answers { Mono.empty() }
        coEvery {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any())
        } returns ResourceNotFoundException(attributeNotFoundMessage(expandedAttr)).left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$temporalEntityAttributeName/$attributeInstanceId")
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "detail":"${attributeNotFoundMessage(expandedAttr)}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found"
                }
                """.trimIndent()
            )

        coVerify {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(entityUri, expandedAttr)
        }
        confirmVerified(temporalEntityAttributeService)
    }

    @Test
    fun `delete attribute instance temporal should return 404 if attributeInstanceId is not found`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, JsonLdUtils.NGSILD_CORE_CONTEXT)!!

        coEvery { entityAccessRightsService.canWriteEntity(any(), any()) } answers { Unit.right() }
        coEvery {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any())
        } returns Unit.right()
        coEvery {
            attributeInstanceService.deleteEntityAttributeInstance(any(), any(), any())
        } returns ResourceNotFoundException(instanceNotFoundMessage(attributeInstanceId.toString())).left()

        webClient
            .method(HttpMethod.DELETE)
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs/$temporalEntityAttributeName/$attributeInstanceId")
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "detail":"${instanceNotFoundMessage(attributeInstanceId.toString())}",
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found"
                }
                """.trimIndent()
            )

        coVerify { temporalEntityAttributeService.checkEntityAndAttributeExistence(entityUri, expandedAttr) }
        coVerify {
            attributeInstanceService.deleteEntityAttributeInstance(entityUri, expandedAttr, attributeInstanceId)
        }
        coVerify {
            entityAccessRightsService.canWriteEntity(
                eq(Some("0768A6D5-D87B-4209-9A22-8C40A8961A79")),
                eq(entityUri)
            )
        }
        confirmVerified(temporalEntityAttributeService, attributeInstanceService, entityAccessRightsService)
    }

    @Test
    fun `delete attribute instance temporal should return 403 if user is not allowed`() {
        val expandedAttr = JsonLdUtils.expandJsonLdTerm(temporalEntityAttributeName, JsonLdUtils.NGSILD_CORE_CONTEXT)!!

        coEvery {
            entityAccessRightsService.canWriteEntity(any(), any())
        } answers { AccessDeniedException("User forbidden write access to entity $entityUri").left() }
        coEvery {
            temporalEntityAttributeService.checkEntityAndAttributeExistence(any(), any())
        } returns Unit.right()

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

        coVerify { temporalEntityAttributeService.checkEntityAndAttributeExistence(entityUri, expandedAttr) }
        coVerify {
            entityAccessRightsService.canWriteEntity(
                eq(Some("0768A6D5-D87B-4209-9A22-8C40A8961A79")),
                eq(entityUri)
            )
        }
        confirmVerified(temporalEntityAttributeService, entityAccessRightsService)
    }

    private fun buildDefaultQueryParams(): TemporalEntitiesQuery =
        TemporalEntitiesQuery(
            queryParams = QueryParams(offset = 0, limit = 0),
            temporalQuery = TemporalQuery(),
            withTemporalValues = false,
            withAudit = false
        )
}
