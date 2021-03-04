package com.egm.stellio.search.web

import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.TemporalEntityService
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.UUID

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockUser
class TemporalEntityHandlerTests {

    private val incomingAttrExpandedName = "https://ontology.eglobalmark.com/apic#incoming"
    private val outgoingAttrExpandedName = "https://ontology.eglobalmark.com/apic#outgoing"

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    private lateinit var apicHeaderLink: String

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityService: TemporalEntityService

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @BeforeAll
    fun configureWebClientDefaults() {
        apicHeaderLink = "<$apicContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json"

        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @Test
    fun `it should return a 204 if temporal entity fragment is valid`() {
        val jsonLdObservation = loadSampleData("observation.jsonld")
        val parsedJsonLdObservation = JsonUtils.deserializeObject(jsonLdObservation)
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )
        every { attributeInstanceService.addAttributeInstances(any(), any(), any(), any()) } returns Mono.just(1)

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri/attrs")
            .header("Link", "<$apicContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(jsonLdObservation))
            .exchange()
            .expectStatus().isNoContent

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(entityUri),
                eq("https://ontology.eglobalmark.com/apic#incoming")
            )
        }
        verify {
            attributeInstanceService.addAttributeInstances(
                eq(temporalEntityAttributeUuid),
                eq("incoming"),
                match {
                    it.size == 4
                },
                parsedJsonLdObservation
            )
        }
        confirmVerified(temporalEntityAttributeService)
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should return a 400 if temporal entity fragment is badly formed`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/entityId/attrs")
            .header("Link", "<$apicContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue("{ \"id\": \"bad\" }"))
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Unable to parse input payload"
                }
                """
            )
    }

    @Test
    fun `it should raise a 400 if timerel is present without time query param`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=before")
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
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?time=2020-10-29T18:00:00Z")
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
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = entityUri,
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any(), any()) } returns Mono.just(emptyList())
        every { temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `it should raise a 400 if timerel is between and no endTime provided`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=startTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'endTime' request parameter is mandatory if 'timerel' is 'between'"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if time is not parseable`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=before&time=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'time' parameter is not a valid date"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if timerel is not a valid value`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=befor&time=badTime")
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
    fun `it should raise a 400 if timerel is between and endTime is not parseable`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=endTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"'endTime' parameter is not a valid date"
                } 
                """
            )
    }

    @Test
    fun `it should raise a 400 if one of time bucket or aggregate is missing`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=after&time=2020-01-31T07:31:39Z&timeBucket=1 minute")
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
        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/entityId?" +
                    "timerel=after&time=2020-01-31T07:31:39Z&timeBucket=1 minute&aggregate=unknown"
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
        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.empty()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
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
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = entityUri,
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )

        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any(), any()) } returns Mono.just(emptyList())
        every { temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
            )
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        verify {
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                match { entityTemporalProperty -> entityTemporalProperty.entityId == entityUri },
                false
            )
        }

        verify { temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), false) }

        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should return a single entity if the entity has two temporal properties`() {
        val entityTemporalProperty1 = TemporalEntityAttribute(
            entityId = entityUri,
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        val entityTemporalProperty2 = TemporalEntityAttribute(
            entityId = entityUri,
            type = "BeeHive",
            attributeName = "outgoing",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty1,
            entityTemporalProperty2
        )
        every { attributeInstanceService.search(any(), any(), any()) } returns Mono.just(emptyList())
        every { temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap

        verify {
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                match { entityTemporalProperty -> entityTemporalProperty.entityId == entityUri },
                false
            )
        }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should return an entity with two temporal properties evolution`() {
        mockWithIncomingAndOutgoingTemporalProperties(false)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
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
        mockWithIncomingAndOutgoingTemporalProperties(false)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
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
        mockWithIncomingAndOutgoingTemporalProperties(true)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z&options=temporalValues"
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
        val entityTemporalProperties = listOf(
            incomingAttrExpandedName,
            outgoingAttrExpandedName
        ).map {
            TemporalEntityAttribute(
                entityId = entityUri,
                type = "BeeHive",
                attributeName = it,
                attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
            )
        }
        val entityFileName = if (withTemporalValues)
            "beehive_with_two_temporal_attributes_evolution_temporal_values.jsonld"
        else
            "beehive_with_two_temporal_attributes_evolution.jsonld"

        val entityWith2temporalEvolutions = deserializeObject(loadSampleData(entityFileName))
        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperties[0],
            entityTemporalProperties[1]
        )

        val attributes = listOf(
            incomingAttrExpandedName,
            outgoingAttrExpandedName
        )
        val values = listOf(Pair(1543, "2020-01-24T13:01:22.066Z"), Pair(1600, "2020-01-24T14:01:22.066Z"))
        val attInstanceResults = attributes.flatMap {
            values.map {
                SimplifiedAttributeInstanceResult(
                    value = it.first,
                    observedAt = ZonedDateTime.parse(it.second)
                )
            }
        }

        listOf(Pair(0, entityTemporalProperties[0]), Pair(2, entityTemporalProperties[1])).forEach {
            every {
                attributeInstanceService.search(any(), it.second, withTemporalValues)
            } returns Mono.just(listOf(attInstanceResults[it.first], attInstanceResults[it.first + 1]))
        }

        every {
            temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any())
        } returns entityWith2temporalEvolutions
    }

    @Test
    fun `it should correctly dispatch queries on entities having properties with a raw value`() {
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = entityUri,
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )

        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any(), any()) } returns Mono.just(emptyList())
        every { temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap

        verify {
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                match { entityTemporalProperty -> entityTemporalProperty.entityId == entityUri },
                false
            )
        }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should only return attributes asked as request parameters`() {
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = entityUri,
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )

        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any(), any()) } returns Mono.just(emptyList())
        every { temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityUri?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z&attrs=incoming"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap

        verify {
            temporalEntityAttributeService.getForEntity(
                entityUri,
                setOf("https://uri.etsi.org/ngsi-ld/default-context/incoming")
            )
        }

        verify {
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                match { entityTemporalProperty -> entityTemporalProperty.entityId == entityUri },
                false
            )
        }

        confirmVerified(temporalEntityAttributeService, attributeInstanceService)
    }

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Sensor:0022CCC")
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `it should parse a query containing one attrs parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "outgoing")

        val temporalQuery = buildTemporalQuery(queryParams, apicContext!!)

        assertTrue(temporalQuery.expandedAttrs.size == 1)
        assertTrue(temporalQuery.expandedAttrs.contains(outgoingAttrExpandedName))
    }

    @Test
    fun `it should parse a query containing two attrs parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "incoming,outgoing")

        val temporalQuery = buildTemporalQuery(queryParams, apicContext!!)

        assertTrue(temporalQuery.expandedAttrs.size == 2)
        assertTrue(
            temporalQuery.expandedAttrs.containsAll(
                listOf(
                    outgoingAttrExpandedName,
                    incomingAttrExpandedName
                )
            )
        )
    }

    @Test
    fun `it should parse a query containing no attrs parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")

        val temporalQuery = buildTemporalQuery(queryParams, apicContext!!)

        assertTrue(temporalQuery.expandedAttrs.isEmpty())
    }

    @Test
    fun `it should parse lastN parameter if it is a positive integer`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "2")

        val temporalQuery = buildTemporalQuery(queryParams, apicContext!!)

        assertTrue(temporalQuery.lastN == 2)
    }

    @Test
    fun `it should ignore lastN parameter if it is not an integer`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "A")

        val temporalQuery = buildTemporalQuery(queryParams, apicContext!!)

        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should ignore lastN parameter if it is not a positive integer`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "-2")

        val temporalQuery = buildTemporalQuery(queryParams, apicContext!!)

        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should treat time and timerel properties as optional`() {
        val queryParams = LinkedMultiValueMap<String, String>()

        val temporalQuery = buildTemporalQuery(queryParams, apicContext!!)

        assertEquals(null, temporalQuery.time)
        assertEquals(null, temporalQuery.timerel)
    }
}
