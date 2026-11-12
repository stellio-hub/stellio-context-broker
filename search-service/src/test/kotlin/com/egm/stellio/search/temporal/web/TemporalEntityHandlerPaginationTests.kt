package com.egm.stellio.search.temporal.web

import arrow.core.Either
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.shared.util.loadAndExpandSampleData
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(SearchProperties::class)
@TestPropertySource(
    properties = ["application.pagination.temporal-limit = 5", "application.pagination.temporal-limit-max = 10"]
)
class TemporalEntityHandlerPaginationTests : TemporalEntityHandlerTestCommon() {

    private val timeAt = "2019-01-01T00:00:00Z"
    private val lastN = "10"
    private val entityId = "urn:ngsi-ld:BeeHive:TESTC"

    private suspend fun mockQueryEntitiesService(hasMore: Boolean = false) {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/beehive_create_temporal_entity.jsonld"
        )
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")
        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Either.Right(
            Triple(
                listOf(firstTemporalEntity, secondTemporalEntity),
                2,
                hasMore
            )
        )
    }

    private suspend fun mockQueryEntityService(hasMore: Boolean = false) {
        val firstTemporalEntity = loadAndExpandSampleData("/temporal/beehive_create_temporal_entity.jsonld")
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns Either.Right(
            firstTemporalEntity to hasMore
        )
    }

    @Test
    fun `query temporal entity should limit to temporal pagination limit`() = runTest {
        mockQueryEntitiesService()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "type=BeeHive"
            )
            .exchange()

        coVerify {
            temporalQueryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.instanceLimit == 5 &&
                        !temporalEntitiesQuery.temporalQuery.hasLastN()
                }
            )
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["firstN", "lastN"])
    fun `getForEntities and getForEntity should reject page sizes above the maximum`(parameter: String) = runTest {
        val urls = listOf(
            "/ngsi-ld/v1/temporal/entities?timerel=after&timeAt=$timeAt&type=BeeHive&$parameter=12",
            "/ngsi-ld/v1/temporal/entities/$entityId?$parameter=12"
        )
        urls.forEach { url ->
            webClient.get().uri(url).exchange()
                .expectStatus().isForbidden
                .expectBody()
                .jsonPath("$.type").isEqualTo("https://uri.etsi.org/ngsi-ld/errors/TooManyResults")
                .jsonPath("$.title").isEqualTo("The '$parameter' value (12) exceeds the maximum temporal limit (10)")
        }
        coVerify(exactly = 0) {
            temporalQueryService.queryTemporalEntities(any())
            temporalQueryService.queryTemporalEntity(any(), any())
        }
    }

    @Test
    fun `getForEntities should preserve a requested page size above the default in links`() = runTest {
        mockQueryEntitiesService(true)

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities?timerel=after&timeAt=$timeAt&type=BeeHive&firstN=7")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueMatches("Link", ".*firstN=7.*offsetN=7.*intervalafter.*")

        coVerify {
            temporalQueryService.queryTemporalEntities(match { it.temporalQuery.instanceLimit == 7 })
        }
    }

    @Test
    fun `query temporal entities should return 200 if pagination was not triggered`() = runTest {
        mockQueryEntitiesService()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "type=BeeHive"
            )
            .exchange()
            .expectStatus().isEqualTo(200)
    }

    @Test
    fun `getForEntities should return 200 and temporal links for full pages`() = runTest {
        mockQueryEntitiesService(true)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "type=BeeHive"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader().doesNotExist("Content-Range")
            .expectHeader().valueMatches("Link", ".*offsetN=5.*rel=\"intervalafter\".*")
    }

    @Test
    fun `getForEntities should link towards earlier timestamps with lastN`() = runTest {
        mockQueryEntitiesService(true)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "lastN=$lastN&" +
                    "type=BeeHive"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader().doesNotExist("Content-Range")
            .expectHeader().valueMatches("Link", ".*lastN=10.*offsetN=10.*rel=\"intervalbefore\".*")
    }

    @Test
    fun `getForEntity should return 200 and temporal links for full pages`() = runTest {
        mockQueryEntityService(true)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityId?" +
                    "timerel=after&timeAt=$timeAt"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader().doesNotExist("Content-Range")
            .expectHeader().valueMatches("Link", ".*entities/$entityId.*offsetN=5.*intervalafter.*")
    }
}
