package com.egm.stellio.search.web

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.loadAndParseSampleData
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.*

@AutoConfigureWebTestClient(timeout = "30000")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@WithMockUser
class TemporalEntityHandlerTests {

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

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(temporalEntityAttributeUuid)
        every { attributeInstanceService.addAttributeInstances(any(), any(), any()) } returns Mono.just(1)

        webClient.post()
                .uri("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:BeeHive:TESTC/attrs")
                .body(BodyInserters.fromValue(jsonLdObservation.inputStream.readAllBytes()))
                .exchange()
                .expectStatus().isNoContent

        verify { temporalEntityAttributeService.getForEntityAndAttribute(eq("urn:ngsi-ld:BeeHive:TESTC"),
            eq("incoming")) }
        verify { attributeInstanceService.addAttributeInstances(eq(temporalEntityAttributeUuid),
            eq("incoming"),
            match {
                it.size == 4
            }
        ) }
        confirmVerified(temporalEntityAttributeService)
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should return a 400 if temporal entity fragment is badly formed`() {

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/entityId/attrs")
            .body(BodyInserters.fromValue("{ \"id\": \"bad\" }"))
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                "\"detail\":\"Unable to expand JSON-LD fragment : { \\\"id\\\": \\\"bad\\\" }\"}")
    }

    @Test
    fun `it should return a 415 if Content-Type is not supported`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/entityId/attrs")
            .header(HttpHeaders.CONTENT_LENGTH, "3495")
            .header(HttpHeaders.CONTENT_TYPE, "application/pdf")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
            .expectBody().isEmpty
    }

    @Test
    fun `it should return a 411 if Content-Length is null`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/entityId/attrs")
            .header(HttpHeaders.CONTENT_TYPE, "application/json")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.LENGTH_REQUIRED)
            .expectBody().isEmpty
    }

    @Test
    fun `it should return a 400 if a service throws a BadRequestDataException`() {

        every { temporalEntityAttributeService.getForEntity(any(), any(), any()) } throws BadRequestDataException("Bad request")

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'timerel and 'time' request parameters are mandatory\"}")
    }

    @Test
    fun `it should raise a 400 if no timerel query param`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?time=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'timerel and 'time' request parameters are mandatory\"}")
    }

    @Test
    fun `it should raise a 400 if no time query param`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'timerel and 'time' request parameters are mandatory\"}")
    }

    @Test
    fun `it should raise a 400 if timerel is between and no endTime provided`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=startTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'endTime' request parameter is mandatory if 'timerel' is 'between'\"}")
    }

    @Test
    fun `it should raise a 400 if time is not parseable`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=before&time=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'time' parameter is not a valid date\"}")
    }

    @Test
    fun `it should raise a 400 if timerel is not a valid value`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=befor&time=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'timerel' is not valid, it should be one of 'before', 'between', or 'after'\"}")
    }

    @Test
    fun `it should raise a 400 if timerel is between and endTime is not parseable`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=endTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'endTime' parameter is not a valid date\"}")
    }

    @Test
    fun `it should raise a 400 if one of time bucket or aggregate is missing`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=after&time=2020-01-31T07:31:39Z&timeBucket=1 minute")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"'timeBucket' and 'aggregate' must both be provided for aggregated queries\"}")
    }

    @Test
    fun `it should raise a 400 if aggregate function is unknown`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=after&time=2020-01-31T07:31:39Z&timeBucket=1 minute&aggregate=unknown")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/BadRequestData\"," +
                    "\"title\":\"The request includes input data which does not meet the requirements of the operation\"," +
                    "\"detail\":\"Value 'unknown' is not supported for 'aggregate' parameter\"}")
    }

    @Test
    fun `it should return a 404 if temporal entity attribute does not exist`() {

        every { temporalEntityAttributeService.getForEntity(any(), any(), any()) } returns Flux.empty()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z")
            .exchange()
            .expectStatus().isNotFound
            .expectBody().json("{\"type\":\"https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound\"," +
                "\"title\":\"The referred resource has not been found\"," +
                "\"detail\":\"Entity entityId was not found\"}")
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid`() {

        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = "entityId",
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )

        every { temporalEntityAttributeService.getForEntity(any(), any(), any()) } returns Flux.just(entityTemporalProperty)
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(loadAndParseSampleData())
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns mockkClass(ExpandedEntity::class, relaxed = true)
        every { temporalEntityAttributeService.addEntityPayload(any(), any()) } returns Mono.just(1)

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z")
            .exchange()
            .expectStatus().isOk

        verify { attributeInstanceService.search(match { temporalQuery ->
            temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                temporalQuery.time.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
        }, match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId" }) }
        confirmVerified(attributeInstanceService)

        verify { entityService.getEntityById(eq("entityId"), any()) }
        confirmVerified(entityService)

        verify { temporalEntityAttributeService.injectTemporalValues(any(), any(), false) }

        verify(timeout = 1000) { temporalEntityAttributeService.addEntityPayload(match {
            it.entityId == "entityId"
        }, match {
            // TODO we need a way to compare payloads with struggling with indents and carriage returns and ....
            it.startsWith("{\"id\":\"urn:ngsi-ld:BeeHive:TESTC\",\"type\":\"BeeHive\"")
        }) }
    }

    @Test
    fun `it should return a single entity if the entity has two temporal properties`() {

        val entityTemporalProperty1 = TemporalEntityAttribute(
            entityId = "entityId",
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        val entityTemporalProperty2 = TemporalEntityAttribute(
            entityId = "entityId",
            type = "BeeHive",
            attributeName = "outgoing",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        val rawEntity = loadAndParseSampleData()
        every { temporalEntityAttributeService.getForEntity(any(), any(), any()) } returns Flux.just(entityTemporalProperty1, entityTemporalProperty2)
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns rawEntity

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z")
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap

        verify { attributeInstanceService.search(match { temporalQuery ->
            temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                    temporalQuery.time.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
        }, match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId" }) }
        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should correctly dispatch queries on entities having properties with a raw value`() {

        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = "entityId",
            type = "BeeHive",
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        val rawEntity = loadAndParseSampleData()

        every { temporalEntityAttributeService.getForEntity(any(), any(), any()) } returns Flux.just(entityTemporalProperty)
        every { attributeInstanceService.search(any(), any()) } returns Mono.just(emptyList())
        every { entityService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
        every { temporalEntityAttributeService.injectTemporalValues(any(), any(), any()) } returns rawEntity

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z")
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap

        verify { attributeInstanceService.search(match { temporalQuery ->
            temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                    temporalQuery.time.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
        }, match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId" }) }
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

        val temporalQuery = buildTemporalQuery(queryParams)

        assertTrue(temporalQuery.attrs.size == 1 && temporalQuery.attrs[0] == "outgoing")
    }

    @Test
    fun `it should parse a query containing two attrs parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "outgoing")
        queryParams.add("attrs", "incoming")

        val temporalQuery = buildTemporalQuery(queryParams)

        assertTrue(temporalQuery.attrs.size == 2 && temporalQuery.attrs.containsAll(listOf("outgoing", "incoming")))
    }
}
