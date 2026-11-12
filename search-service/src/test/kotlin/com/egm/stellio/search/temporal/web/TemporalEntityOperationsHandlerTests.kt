package com.egm.stellio.search.temporal.web

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.service.TemporalQueryService
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.loadAndExpandSampleData
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.time.ZonedDateTime

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityOperationsHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class TemporalEntityOperationsHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var temporalQueryService: TemporalQueryService

    private fun temporalQueryBody(pagination: Map<String, Int> = emptyMap()): String = serializeObject(
        mapOf(
            "type" to "Query",
            "entities" to listOf(mapOf("type" to BEEHIVE_TERM)),
            "temporalQ" to mapOf("timerel" to "after", "timeAt" to "2019-10-17T07:31:39Z") + pagination
        )
    )

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @ParameterizedTest
    @ValueSource(strings = ["firstN", "lastN"])
    fun `queryEntitiesViaPost should reject page sizes above the maximum`(parameter: String) {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(temporalQueryBody(mapOf(parameter to 101))).header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isForbidden
            .expectBody()
            .jsonPath("$.type").isEqualTo("https://uri.etsi.org/ngsi-ld/errors/TooManyResults")
            .jsonPath("$.title").isEqualTo("The '$parameter' value (101) exceeds the maximum temporal limit (100)")
        coVerify(exactly = 0) { temporalQueryService.queryTemporalEntities(any()) }
    }

    @ParameterizedTest
    @ValueSource(strings = ["firstN", "lastN"])
    fun `queryEntitiesViaPost should use body limits and URL offsets`(parameter: String) {
        coEvery { temporalQueryService.queryTemporalEntities(any()) } returns
            Triple(emptyList<ExpandedEntity>(), 1, true).right()
        val response = webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query?offsetN=2")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(temporalQueryBody(mapOf(parameter to 2))).header("Link", APIC_HEADER_LINK)
            .exchange().expectStatus().isOk
            .expectHeader().doesNotExist("Content-Range")
            .expectBody().returnResult()
        val links = response.responseHeaders["Link"]!!
        val relation = if (parameter == "lastN") "intervalbefore" else "intervalafter"
        assertTrue(links.none { it.contains("firstN=") || it.contains("lastN=") })
        assertTrue(
            links.any {
                it.contains("/temporal/entityOperations/query?") &&
                    it.contains("offsetN=4") && it.contains(relation)
            }
        )
        coVerify {
            temporalQueryService.queryTemporalEntities(
                match {
                    it.temporalQuery.instanceLimit == 2 && it.temporalQuery.offsetN == 2 &&
                        it.temporalQuery.hasLastN() == (parameter == "lastN")
                }
            )
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["firstN", "lastN"])
    fun `queryEntitiesViaPost should reject limits in the query string`(parameter: String) {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query?$parameter=3")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(temporalQueryBody(mapOf(parameter to 2))).header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isBadRequest
        coVerify(exactly = 0) { temporalQueryService.queryTemporalEntities(any()) }
    }

    @Test
    fun `queryEntitiesViaPost should reject offsetN in the body`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query?offsetN=3")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(temporalQueryBody(mapOf("offsetN" to 2))).header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isBadRequest
        coVerify(exactly = 0) { temporalQueryService.queryTemporalEntities(any()) }
    }

    @Test
    fun `query temporal entities should return 200 and retrieve the requested temporal attributes`() {
        val temporalQuery = buildDefaultTestTemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, false))

        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "$BEEHIVE_TERM,$APIARY_TERM"
                }],
                "attrs": ["incoming", "outgoing"],
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2019-10-17T07:31:39Z",
                    "endTimeAt": "2019-10-18T07:31:39Z"
                }
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query?options=temporalValues")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(query)
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk

        coVerify {
            temporalQueryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    val entitiesQueryFromPost = temporalEntitiesQuery.entitiesQuery as EntitiesQueryFromPost
                    temporalEntitiesQuery.entitiesQuery.paginationQuery.limit == 30 &&
                        temporalEntitiesQuery.entitiesQuery.paginationQuery.offset == 0 &&
                        entitiesQueryFromPost.entitySelectors!!.size == 1 &&
                        entitiesQueryFromPost.entitySelectors[0].id == null &&
                        entitiesQueryFromPost.entitySelectors[0].typeSelection == "$BEEHIVE_IRI,$APIARY_IRI" &&
                        temporalEntitiesQuery.entitiesQuery.attrs == setOf(INCOMING_IRI, OUTGOING_IRI) &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.TEMPORAL_VALUES
                }
            )
        }
    }

    @Test
    fun `query temporal entities should use the context embedded in the request body`() {
        val temporalQuery = buildDefaultTestTemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, false))

        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "$BEEHIVE_TERM,$APIARY_TERM"
                }],
                "attrs": ["incoming", "outgoing"],
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2019-10-17T07:31:39Z",
                    "endTimeAt": "2019-10-18T07:31:39Z"
                },
                "@context": "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query?options=temporalValues")
            .bodyValue(query)
            .exchange()
            .expectStatus().isOk

        coVerify {
            temporalQueryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    val entitiesQueryFromPost = temporalEntitiesQuery.entitiesQuery as EntitiesQueryFromPost
                    entitiesQueryFromPost.entitySelectors!![0].typeSelection == "$BEEHIVE_IRI,$APIARY_IRI" &&
                        temporalEntitiesQuery.entitiesQuery.attrs == setOf(INCOMING_IRI, OUTGOING_IRI) &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery
                }
            )
        }
    }

    @Test
    fun `query temporal entities should return 200 and the number of results when count is asked`() {
        val temporalQuery = buildDefaultTestTemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, false))

        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "$BEEHIVE_TERM,$APIARY_TERM"
                }],
                "attrs": ["incoming", "outgoing"],
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2019-10-17T07:31:39Z",
                    "endTimeAt": "2019-10-18T07:31:39Z"
                }
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query?options=temporalValues&count=true")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(query)
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "2")

        coVerify {
            temporalQueryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    val entitiesQueryFromPost = temporalEntitiesQuery.entitiesQuery as EntitiesQueryFromPost
                    temporalEntitiesQuery.entitiesQuery.paginationQuery.limit == 30 &&
                        temporalEntitiesQuery.entitiesQuery.paginationQuery.offset == 0 &&
                        entitiesQueryFromPost.entitySelectors!!.size == 1 &&
                        entitiesQueryFromPost.entitySelectors[0].id == null &&
                        entitiesQueryFromPost.entitySelectors[0].typeSelection == "$BEEHIVE_IRI,$APIARY_IRI" &&
                        temporalEntitiesQuery.entitiesQuery.attrs == setOf(INCOMING_IRI, OUTGOING_IRI) &&
                        temporalEntitiesQuery.entitiesQuery.paginationQuery.count &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.TEMPORAL_VALUES
                }
            )
        }

        confirmVerified(temporalQueryService)
    }

    @Test
    fun `query temporal entities should return 400 when required parameters are missing`() {
        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "$BEEHIVE_IRI"
                }],
                "temporalQ": {
                    "timerel": "before"
                }
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(query)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"'timerel' and 'time' must be used in conjunction"
                }
                """
            )
    }

    @Test
    fun `query temporal entities should return entities with only the picked attributes`() = runTest {
        val expandedEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Triple(listOf(expandedEntity), 1, false).right()

        val query = """
            {
                "type": "Query",
                "entities": [{"type": "$BEEHIVE_TERM"}],
                "pick": ["incoming"],
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2019-10-17T07:31:39Z",
                    "endTimeAt": "2019-10-18T07:31:39Z"
                }
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(query)
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$[0].incoming").exists()
            .jsonPath("$[0].outgoing").doesNotExist()
    }

    @Test
    fun `query temporal entities should return entities without the omitted attributes`() = runTest {
        val expandedEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Triple(listOf(expandedEntity), 1, false).right()

        val query = """
            {
                "type": "Query",
                "entities": [{"type": "$BEEHIVE_TERM"}],
                "omit": ["incoming"],
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2019-10-17T07:31:39Z",
                    "endTimeAt": "2019-10-18T07:31:39Z"
                }
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(query)
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$[0].outgoing").exists()
            .jsonPath("$[0].incoming").doesNotExist()
    }

    @Test
    fun `query temporal entities should return an empty list when no member matches the pick parameter`() = runTest {
        val expandedEntity = loadAndExpandSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Triple(listOf(expandedEntity), 1, false).right()

        val query = """
            {
                "type": "Query",
                "entities": [{"type": "$BEEHIVE_TERM"}],
                "pick": ["nonExistentAttribute"],
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2019-10-17T07:31:39Z",
                    "endTimeAt": "2019-10-18T07:31:39Z"
                }
            }
        """.trimIndent()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(query)
            .header("Link", APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody().json("[]")
    }
}
