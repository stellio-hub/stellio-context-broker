package com.egm.stellio.search.web

import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.util.QueryUtils
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.reactive.function.BodyInserters
import java.time.ZonedDateTime

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityOperationsHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockUser
class TemporalEntityOperationsHandlerTests {

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    private lateinit var apicHeaderLink: String

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var queryUtils: QueryUtils

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val secondEntityUri = "urn:ngsi-ld:BeeHive:TESTB".toUri()

    @BeforeAll
    fun configureWebClientDefaults() {
        apicHeaderLink = buildContextLinkHeader(apicContext!!)

        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @Test
    fun `it should raise a 400 for query temporal entities if timerel is present without time in query params`() {
        val queryParams = mapOf("timerel" to "before")
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
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
    fun `it should raise a 400 for query temporal entities if neither type nor attrs is present in query params`() {
        val queryParams = mapOf("timerel" to "after", "time" to "2019-10-17T07:31:39Z")
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"The request includes input data which does not meet the requirements of the operation",
                    "detail":"Either type or attrs need to be present in request parameters"
                }
                """
            )
    }

    @Test
    fun `it should return a 200 with empty payload if no temporal attribute is found`() {
        val queryParams = mapOf(
            "timerel" to "between",
            "time" to "2019-10-17T07:31:39Z",
            "endTime" to "2019-10-18T07:31:39Z",
            "type" to "BeeHive"
        )
        coEvery { queryUtils.queryTemporalEntities(any(), any(), any(), any(), any()) } returns emptyList()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")

        coVerify {
            queryUtils.queryTemporalEntities(
                emptySet(),
                setOf("https://ontology.eglobalmark.com/apic#BeeHive"),
                match {
                    it.expandedAttrs == emptySet<String>()
                },
                false,
                listOf(apicContext!!)
            )
        }

        confirmVerified(queryUtils)
    }

    @Test
    fun `it should return 200 with jsonld response body for query temporal entities`() {
        val queryParams = mapOf(
            "timerel" to "between",
            "time" to "2019-10-17T07:31:39Z",
            "endTime" to "2019-10-18T07:31:39Z",
            "type" to "BeeHive"
        )
        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")

        coEvery { queryUtils.queryTemporalEntities(any(), any(), any(), any(), any()) } returns
            listOf(firstTemporalEntity, secondTemporalEntity)

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$").isArray
            .jsonPath("$.length()").isEqualTo(2)
            .jsonPath("$[0].@context").exists()
            .jsonPath("$[1].@context").exists()

        coVerify {
            queryUtils.queryTemporalEntities(
                emptySet(),
                setOf("https://ontology.eglobalmark.com/apic#BeeHive"),
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BETWEEN &&
                        temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                false,
                listOf(apicContext!!)
            )
        }

        confirmVerified(queryUtils)
    }

    @Test
    fun `it should return a 200 and retrieve temporal attributes requested by the query parameters`() {
        val queryParams = mapOf(
            "options" to "temporalValues",
            "timerel" to "between",
            "time" to "2019-10-17T07:31:39Z",
            "endTime" to "2019-10-18T07:31:39Z",
            "type" to "BeeHive,Apiary",
            "id" to "$entityUri,$secondEntityUri",
            "attrs" to "incoming,outgoing"
        )

        coEvery { queryUtils.queryTemporalEntities(any(), any(), any(), any(), any()) } returns emptyList()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify {
            queryUtils.queryTemporalEntities(
                setOf(entityUri, secondEntityUri),
                setOf("https://ontology.eglobalmark.com/apic#BeeHive", "https://ontology.eglobalmark.com/apic#Apiary"),
                match {
                    it.expandedAttrs == setOf(
                        "https://ontology.eglobalmark.com/apic#incoming",
                        "https://ontology.eglobalmark.com/apic#outgoing"
                    )
                },
                true,
                listOf(apicContext!!)
            )
        }
        confirmVerified(queryUtils)
    }

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .exchange()
            .expectStatus().isForbidden
    }
}
