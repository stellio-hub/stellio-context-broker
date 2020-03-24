package com.egm.stellio.search.web

import com.egm.stellio.search.loadAndParseSampleData
import com.egm.stellio.search.model.EntityTemporalProperty
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.ContextRegistryService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.ObservationService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.*
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.OffsetDateTime

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@WithMockUser
class TemporalEntityHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var observationService: ObservationService

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var contextRegistryService: ContextRegistryService

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

        every { observationService.create(any()) } returns Mono.just(1)

        webClient.post()
                .uri("/ngsi-ld/v1/temporal/entities/entityId/attrs")
                .body(BodyInserters.fromValue(jsonLdObservation.inputStream.readAllBytes()))
                .exchange()
                .expectStatus().isNoContent

        verify { observationService.create(match {
            it.observedBy == "urn:sosa:Sensor:10e2073a01080065"
        }) }
        confirmVerified(observationService)
    }

    @Test
    fun `it should return a 400 if temporal entity fragment is badly formed`() {

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/entityId/attrs")
            .body(BodyInserters.fromValue("{ \"id\": \"bad\" }"))
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("Received content misses one or more required attributes")
    }

    @Test
    fun `it should return a 415 if Content-Type is not supported`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entities/entityId/attrs")
            .header(HttpHeaders.CONTENT_TYPE, "application/pdf")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
            .expectBody<String>().isEqualTo("Content-Type header must be one of 'application/json' or 'application/ld+json' (was application/pdf)")
    }

    @Test
    fun `it should return a 400 if a service throws a BadRequestDataException`() {

        every { entityService.getForEntity(any(), any()) } throws BadRequestDataException("Bad request")

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId")
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `it should raise a 400 if no timerel query param`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?time=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("'timerel and 'time' request parameters are mandatory")
    }

    @Test
    fun `it should raise a 400 if no time query param`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=before")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("'timerel and 'time' request parameters are mandatory")
    }

    @Test
    fun `it should raise a 400 if timerel is between and no endTime provided`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=startTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("'endTime' request parameter is mandatory if 'timerel' is 'between'")
    }

    @Test
    fun `it should raise a 400 if time is not parseable`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=before&time=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("'time' parameter is not a valid date")
    }

    @Test
    fun `it should raise a 400 if timerel is not a valid value`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=befor&time=badTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("'timerel' is not valid, it should be one of 'before', 'between', or 'after'")
    }

    @Test
    fun `it should raise a 400 if timerel is between and endTime is not parseable`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=endTime")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("'endTime' parameter is not a valid date")
    }

    @Test
    fun `it should raise a 400 if one of time bucket or aggregate is missing`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=after&time=2020-01-31T07:31:39Z&timeBucket=1 minute")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("'timeBucket' and 'aggregate' must both be provided for aggregated queries")
    }

    @Test
    fun `it should raise a 400 if aggregate function is unknown`() {

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=after&time=2020-01-31T07:31:39Z&timeBucket=1 minute&aggregate=unknown")
            .exchange()
            .expectStatus().isBadRequest
            .expectBody<String>().isEqualTo("Value 'unknown' is not supported for 'aggregate' parameter")
    }

    @Test
    fun `it should return a 200 if minimal required parameters are valid`() {

        val entityTemporalProperty = EntityTemporalProperty(
            entityId = "entityId",
            type = "BeeHive",
            attributeName = "incoming",
            observedBy = "sensorId"
        )
        every { entityService.getForEntity(any(), any()) } returns Flux.just(entityTemporalProperty)
        every { observationService.search(any(), any()) } returns Mono.just(emptyList())
        every { contextRegistryService.getEntityById(any(), any()) } returns Mono.just(loadAndParseSampleData())
        every { entityService.injectTemporalValues(any(), any()) } returns Pair(emptyMap(), emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z")
            .exchange()
            .expectStatus().isOk

        verify { observationService.search(match { temporalQuery ->
            temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                temporalQuery.time.isEqual(OffsetDateTime.parse("2019-10-17T07:31:39Z"))
        }, match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId" }) }
        confirmVerified(observationService)

        verify { contextRegistryService.getEntityById(eq("entityId"), any()) }
        confirmVerified(contextRegistryService)
    }

    @Test
    fun `it should return a single entity if the entity has two temporal properties`() {

        val entityTemporalProperty1 = EntityTemporalProperty(
            entityId = "entityId",
            type = "BeeHive",
            attributeName = "incoming",
            observedBy = "sensorId1"
        )
        val entityTemporalProperty2 = EntityTemporalProperty(
            entityId = "entityId",
            type = "BeeHive",
            attributeName = "outgoing",
            observedBy = "sensorId2"
        )
        val rawEntity = loadAndParseSampleData()
        every { entityService.getForEntity(any(), any()) } returns Flux.just(entityTemporalProperty1, entityTemporalProperty2)
        every { observationService.search(any(), any()) } returns Mono.just(emptyList())
        every { contextRegistryService.getEntityById(any(), any()) } returns Mono.just(rawEntity)
        every { entityService.injectTemporalValues(any(), any()) } returns rawEntity

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/entityId?timerel=between&time=2019-10-17T07:31:39Z&endTime=2019-10-18T07:31:39Z")
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$").isMap

        verify { observationService.search(match { temporalQuery ->
            temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                    temporalQuery.time.isEqual(OffsetDateTime.parse("2019-10-17T07:31:39Z"))
        }, match { entityTemporalProperty -> entityTemporalProperty.entityId == "entityId" }) }
        confirmVerified(observationService)
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
