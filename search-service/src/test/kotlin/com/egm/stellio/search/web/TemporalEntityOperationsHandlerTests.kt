package com.egm.stellio.search.web

import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import reactor.core.publisher.Mono
import java.time.ZonedDateTime

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityOperationsHandler::class)
@Import(WebSecurityTestConfig::class)
@WithMockUser
class TemporalEntityOperationsHandlerTests {

    private lateinit var apicHeaderLink: String

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var queryService: QueryService

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
    private val incomingAttrExpandedName = "https://ontology.eglobalmark.com/apic#incoming"
    private val outgoingAttrExpandedName = "https://ontology.eglobalmark.com/apic#outgoing"

    @Test
    fun `it should return a 200 and retrieve requested temporal attributes`() {
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            time = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTime = ZonedDateTime.parse("2019-10-18T07:31:39Z"),
            expandedAttrs = setOf(incomingAttrExpandedName, outgoingAttrExpandedName)
        )

        every { temporalEntityAttributeService.getCountForEntities(any(), any(), any()) } answers { Mono.just(2) }
        every { queryService.parseAndCheckQueryParams(any(), any()) } returns
            TemporalEntitiesQuery(
                ids = emptySet(),
                types = setOf("BeeHive", "Apiary"),
                temporalQuery = temporalQuery,
                withTemporalValues = true,
                limit = 1,
                offset = 0,
                false
            )
        coEvery { queryService.queryTemporalEntities(any(), any()) } returns emptyList()

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("options", "temporalValues")
        queryParams.add("timerel", "between")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("endTime", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        verify {
            queryService.parseAndCheckQueryParams(
                queryParams,
                APIC_COMPOUND_CONTEXT
            )
        }
        coVerify {
            queryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.limit == 1 &&
                        temporalEntitiesQuery.offset == 0 &&
                        temporalEntitiesQuery.ids.isEmpty() &&
                        temporalEntitiesQuery.types == setOf("BeeHive", "Apiary") &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.withTemporalValues
                },
                eq(APIC_COMPOUND_CONTEXT)
            )
        }

        confirmVerified(queryService)
    }

    @Test
    fun `it should return a 200 and the number of results if count is asked for`() {
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.BETWEEN,
            time = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
            endTime = ZonedDateTime.parse("2019-10-18T07:31:39Z"),
            expandedAttrs = setOf(incomingAttrExpandedName, outgoingAttrExpandedName)
        )

        every { temporalEntityAttributeService.getCountForEntities(any(), any(), any()) } answers { Mono.just(2) }
        every { queryService.parseAndCheckQueryParams(any(), any()) } returns
            TemporalEntitiesQuery(
                ids = emptySet(),
                types = setOf("BeeHive", "Apiary"),
                temporalQuery = temporalQuery,
                withTemporalValues = true,
                limit = 0,
                offset = 1,
                true
            )

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("options", "temporalValues")
        queryParams.add("timerel", "between")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("endTime", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "2")

        verify {
            queryService.parseAndCheckQueryParams(
                queryParams,
                APIC_COMPOUND_CONTEXT
            )
        }
        coVerify {
            queryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.limit == 0 &&
                        temporalEntitiesQuery.offset == 1 &&
                        temporalEntitiesQuery.ids.isEmpty() &&
                        temporalEntitiesQuery.types == setOf("BeeHive", "Apiary") &&
                        temporalEntitiesQuery.temporalQuery == temporalQuery &&
                        temporalEntitiesQuery.withTemporalValues &&
                        temporalEntitiesQuery.count == true
                },
                eq(APIC_COMPOUND_CONTEXT)
            )
        }

        confirmVerified(queryService)
    }

    @Test
    fun `it should raise a 400 if required parameters are missing`() {
        val queryParams = mapOf("timerel" to "before")

        every { queryService.parseAndCheckQueryParams(any(), any()) } throws BadRequestDataException(
            "'timerel' and 'time' must be used in conjunction"
        )

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
            .expectStatus().isForbidden
    }
}
