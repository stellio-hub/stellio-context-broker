package com.egm.stellio.search.web

import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.model.TemporalValue
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
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

    val incomingAttrExpandedName = "https://ontology.eglobalmark.com/apic#incoming"
    val outgoingAttrExpandedName = "https://ontology.eglobalmark.com/apic#outgoing"

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var entityService: EntityService

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
    fun `it should return a 204 if temporal entity fragment is valid`() {
        val jsonLdObservation = ClassPathResource("/ngsild/observation.jsonld")
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )
        every { attributeInstanceService.addAttributeInstances(any(), any(), any()) } returns Mono.just(1)

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:TESTC/attrs")
            .header("Link", "<$apicContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .contentType(MediaType.APPLICATION_JSON)
            .body(BodyInserters.fromValue(jsonLdObservation.inputStream.readAllBytes()))
            .exchange()
            .expectStatus().isNoContent

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq("urn:ngsi-ld:BeeHive:TESTC".toUri()),
                eq("incoming")
            )
        }
        verify {
            attributeInstanceService.addAttributeInstances(
                eq(temporalEntityAttributeUuid),
                eq("incoming"),
                match {
                    it.size == 4
                }
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
            entityId = "entityId".toUri(),
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(parseSampleDataToJsonLd())
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns mockkClass(
            JsonLdEntity::class,
            relaxed = true
        )
        every { temporalEntityAttributeService.updateEntityPayload(any(), any()) } returns Mono.just(1)
        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId")
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
                "/ngsi-ld/v1/temporal/entities/entityId?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
            )
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound",
                    "title":"The referred resource has not been found",
                    "detail":"${entityNotFoundMessage("entityId")}"
                }
                """
            )
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid`() {
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = "entityId".toUri(),
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )

        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(parseSampleDataToJsonLd())
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns mockkClass(
            JsonLdEntity::class,
            relaxed = true
        )
        every { temporalEntityAttributeService.updateEntityPayload(any(), any()) } returns Mono.just(1)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/entityId?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
            )
            .exchange()
            .expectStatus().isOk

        verify {
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId".toUri() }
            )
        }
        confirmVerified(attributeInstanceService)

        verify { entityService.getEntityById(eq("entityId".toUri()), any()) }
        confirmVerified(entityService)

        verify { temporalEntityAttributeService.injectTemporalValues(any(), any(), false) }

        verify(timeout = 1000) {
            temporalEntityAttributeService.updateEntityPayload(
                match {
                    it == entityTemporalProperty.entityId
                },
                match {
                    // TODO we need a way to compare payloads with struggling with indents and carriage returns and ....
                    it.startsWith("{\"id\":\"urn:ngsi-ld:BeeHive:TESTC\",\"type\":\"BeeHive\"")
                }
            )
        }
    }

    @Test
    fun `it should return a single entity if the entity has two temporal properties`() {
        val entityTemporalProperty1 = TemporalEntityAttribute(
            entityId = "entityId".toUri(),
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        val entityTemporalProperty2 = TemporalEntityAttribute(
            entityId = "entityId".toUri(),
            type = "BeeHive",
            attributeName = "outgoing",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        val rawEntity = parseSampleDataToJsonLd()
        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty1,
            entityTemporalProperty2
        )
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns rawEntity

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/entityId?" +
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
                match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId".toUri() }
            )
        }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should return an entity with two temporal properties evolution`() {
        mockWithIncomingAndOutgoingTemporalProperties(false)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/entityId?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$.incoming.length()").isEqualTo(2)
            .jsonPath("$.outgoing.length()").isEqualTo(2)
            .jsonPath("$.connectsTo").doesNotExist()
    }

    @Test
    fun `it should return an entity with two temporal properties evolution with temporalValues option`() {
        mockWithIncomingAndOutgoingTemporalProperties(true)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/entityId?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z&options=temporalValues"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap
            .jsonPath("$.id").exists()
            .jsonPath("$.type").exists()
            .jsonPath("$..observedAt").doesNotExist()
            .jsonPath("$.incoming.values.length()").isEqualTo(2)
            .jsonPath("$.outgoing.values.length()").isEqualTo(2)
    }

    private fun mockWithIncomingAndOutgoingTemporalProperties(withTemporalValues: Boolean) {
        val entityTemporalProperties = listOf(
            incomingAttrExpandedName,
            outgoingAttrExpandedName
        ).map {
            TemporalEntityAttribute(
                entityId = "entityId".toUri(),
                type = "BeeHive",
                attributeName = it,
                attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
            )
        }
        val rawEntity = parseSampleDataToJsonLd()
        val entityFileName = if (withTemporalValues)
            "beehive_with_two_temporal_attributes_evolution_temporal_values.jsonld"
        else
            "beehive_with_two_temporal_attributes_evolution.jsonld"

        val entityWith2temporalEvolutions = if (withTemporalValues) {
            val entity = parseSampleDataToJsonLd(entityFileName)
            injectTemporalValuesForIncomingAndOutgoing(entity)
        } else {
            parseSampleDataToJsonLd(entityFileName)
        }

        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperties[0],
            entityTemporalProperties[1]
        )

        val attributes = listOf(
            incomingAttrExpandedName,
            outgoingAttrExpandedName
        )
        val values = listOf(Pair(1543, "2020-01-24T13:01:22.066Z"), Pair(1600, "2020-01-24T14:01:22.066Z"))
        val attInstanceResults = attributes.flatMap { attributeName ->
            values.map {
                AttributeInstanceResult(
                    attributeName = attributeName,
                    value = it.first,
                    observedAt = ZonedDateTime.parse(it.second)
                )
            }
        }

        listOf(Pair(0, entityTemporalProperties[0]), Pair(2, entityTemporalProperties[1])).forEach {
            every {
                attributeInstanceService.search(any(), it.second)
            } returns Mono.just(listOf(attInstanceResults[it.first], attInstanceResults[it.first + 1]))
        }

        every { entityService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
        every {
            temporalEntityAttributeService.injectTemporalValues(any(), any(), any())
        } returns entityWith2temporalEvolutions
    }

    private fun injectTemporalValuesForIncomingAndOutgoing(entity: JsonLdEntity): JsonLdEntity {
        listOf(incomingAttrExpandedName, outgoingAttrExpandedName).forEach {
            val propList = entity.properties[it] as MutableList<MutableMap<String, *>>
            val propHasValuesList =
                propList[0][JsonLdUtils.NGSILD_PROPERTY_VALUES] as MutableList<MutableMap<String, *>>
            val incomingHasValuesMap = propHasValuesList[0] as MutableMap<String, MutableList<*>>
            incomingHasValuesMap["@list"] = mutableListOf(
                TemporalValue(1543.toDouble(), "2020-01-24T13:01:22.066Z"),
                TemporalValue(1600.toDouble(), "2020-01-24T14:01:22.066Z")
            )
        }
        return entity
    }

    @Test
    fun `it should correctly dispatch queries on entities having properties with a raw value`() {
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = "entityId".toUri(),
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        val rawEntity = parseSampleDataToJsonLd()

        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns rawEntity

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/entityId?" +
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
                match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId".toUri() }
            )
        }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should only return attributes asked as request parameters`() {
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = "entityId".toUri(),
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        val rawEntity = parseSampleDataToJsonLd()

        every { temporalEntityAttributeService.getForEntity(any(), any()) } returns Flux.just(
            entityTemporalProperty
        )
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns rawEntity

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/entityId?" +
                    "timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z&attrs=incoming"
            )
            .header("Link", "<$apicContext>; rel=http://www.w3.org/ns/json-ld#context; type=application/ld+json")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.name").doesNotExist()
            .jsonPath("$.connectsTo").doesNotExist()
            .jsonPath("$.incoming").isMap

        verify {
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z")) &&
                        temporalQuery.expandedAttrs == setOf(incomingAttrExpandedName)
                },
                match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId".toUri() }
            )
        }
        confirmVerified(attributeInstanceService)
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
