package com.egm.stellio.search.temporal.web

import arrow.core.Either
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.service.TemporalQueryService
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.DEFAULT_DETAIL
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
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

    @Test
    fun `it should return a 200 and retrieve requested temporal attributes`() {
        val temporalQuery = buildDefaultTestTemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, null))

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
                        entitiesQueryFromPost.entitySelectors!![0].id == null &&
                        entitiesQueryFromPost.entitySelectors!![0].typeSelection == "$BEEHIVE_IRI,$APIARY_IRI" &&
                        temporalEntitiesQuery.entitiesQuery.attrs == setOf(INCOMING_IRI, OUTGOING_IRI) &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.TEMPORAL_VALUES
                }
            )
        }
    }

    @Test
    fun `it should return a 200 and the number of results if count is asked for`() {
        val temporalQuery = buildDefaultTestTemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(Triple(emptyList(), 2, null))

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
                        entitiesQueryFromPost.entitySelectors!![0].id == null &&
                        entitiesQueryFromPost.entitySelectors!![0].typeSelection == "$BEEHIVE_IRI,$APIARY_IRI" &&
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
    fun `it should raise a 400 if required parameters are missing`() {
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
            .bodyValue(query)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type":"https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title":"'timerel' and 'time' must be used in conjunction",
                    "detail":"$DEFAULT_DETAIL"
                }
                """
            )
    }
}
