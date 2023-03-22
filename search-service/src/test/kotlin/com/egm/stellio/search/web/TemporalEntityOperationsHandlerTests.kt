package com.egm.stellio.search.web

import arrow.core.Either
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import java.time.ZonedDateTime

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityOperationsHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockCustomUser(name = "Mock User", sub = "0768A6D5-D87B-4209-9A22-8C40A8961A79")
class TemporalEntityOperationsHandlerTests {

    private lateinit var apicHeaderLink: String

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var queryService: QueryService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @BeforeAll
    fun configureWebClientDefaults() {
        apicHeaderLink = buildContextLinkHeader(APIC_COMPOUND_CONTEXT)

        webClient = webClient.mutate()
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    @Test
    fun `it should return a 200 and retrieve requested temporal attributes`() {
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery { queryService.queryTemporalEntities(any(), any()) } returns Either.Right(Pair(emptyList(), 2))

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("options", "temporalValues")
        queryParams.add("timerel", "between")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("endTimeAt", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify {
            queryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.queryParams.limit == 30 &&
                        temporalEntitiesQuery.queryParams.offset == 0 &&
                        temporalEntitiesQuery.queryParams.ids.isEmpty() &&
                        temporalEntitiesQuery.queryParams.type == "$BEEHIVE_TYPE,$APIARY_TYPE" &&
                        temporalEntitiesQuery.queryParams.attrs == setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY) &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.withTemporalValues
                },
                any()
            )
        }
    }

    @Test
    fun `it should return a 200 and the number of results if count is asked for`() {
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
        )

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery { queryService.queryTemporalEntities(any(), any()) } returns Either.Right(Pair(emptyList(), 2))

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("options", "temporalValues")
        queryParams.add("count", "true")
        queryParams.add("timerel", "between")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("endTimeAt", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "2")

        coVerify {
            queryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.queryParams.limit == 30 &&
                        temporalEntitiesQuery.queryParams.offset == 0 &&
                        temporalEntitiesQuery.queryParams.ids.isEmpty() &&
                        temporalEntitiesQuery.queryParams.type == "$BEEHIVE_TYPE,$APIARY_TYPE" &&
                        temporalEntitiesQuery.queryParams.attrs == setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY) &&
                        temporalEntitiesQuery.queryParams.count &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.withTemporalValues
                },
                any()
            )
        }

        confirmVerified(queryService)
    }

    @Test
    fun `it should raise a 400 if required parameters are missing`() {
        val queryParams = mapOf("timerel" to "before", "type" to "Beehive")

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
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .exchange()
            .expectStatus().isUnauthorized
    }
}
