package com.egm.stellio.search.web

import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.service.QueryService
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
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
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters

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
    private lateinit var queryService: QueryService

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
    fun `it should return a 200 and retrieve temporal attributes requested by the query parameters`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("options", "temporalValues")
        queryParams.add("timerel", "between")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("endTime", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")

        coEvery { queryService.queryTemporalEntities(any(), any()) } returns emptyList()

        webClient.post()
            .uri("/ngsi-ld/v1/temporal/entityOperations/query")
            .body(BodyInserters.fromValue(queryParams))
            .header("Link", apicHeaderLink)
            .exchange()
            .expectStatus().isOk

        coVerify {
            queryService.queryTemporalEntities(
                queryParams,
                apicContext!!
            )
        }

        confirmVerified(queryService)
    }

    @Test
    fun `it should raise a 400 if required parameters are missing`() {
        val queryParams = mapOf("timerel" to "before")

        coEvery { queryService.queryTemporalEntities(any(), any()) } throws BadRequestDataException(
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
