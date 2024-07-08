package com.egm.stellio.search.web

import arrow.core.Either
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.loadAndExpandSampleData
import com.egm.stellio.shared.util.loadSampleData
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(SearchProperties::class)
@TestPropertySource(
    properties =
    [
        "application.pagination.temporal-limit = 5"
    ]
)
class TemporalEntityHandlerPaginationTests : TemporalEntityHandlerTestCommon() {

    val timeAt = "2019-01-01T00:00:00Z"
    val endTimeAt = "2021-01-01T00:00:00Z"
    private val mostRecentTimestamp = "2020-01-01T00:05:00Z" // from discrimination attribute
    private val leastRecentTimestamp = "2020-01-01T00:01:00Z"

    private suspend fun setupTemporalTestWithNonTruncatedValue() {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/pagination/beehive_with_four_temporal_attributes_evolution.jsonld"
        )
        mockService(firstTemporalEntity)
    }

    private suspend fun setupTemporalPaginationTest() {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/pagination/beehive_with_five_temporal_attributes_evolution.jsonld"
        )
        mockService(firstTemporalEntity)
    }
    private suspend fun setupTemporalPaginationWithLastNTest() {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/pagination/beehive_with_five_temporal_attributes_evolution_temporal_values_and_lastN.jsonld"
        )
        mockService(firstTemporalEntity)
    }

    private suspend fun setupTemporalPaginationTestwithTemporalValues() {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/pagination/beehive_with_five_temporal_attributes_evolution_temporal_values.jsonld"
        )

        mockService(firstTemporalEntity)
    }

    private suspend fun mockService(firstTemporalEntity: ExpandedEntity) {
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2))
    }

    @Test
    fun `query temporal entity should limit to temporalPaginationLimit`() =
        runTest {
            setupTemporalTestWithNonTruncatedValue()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(200)

            coVerify {
                queryService.queryTemporalEntities(
                    match { temporalEntitiesQuery ->
                        temporalEntitiesQuery.temporalQuery.instanceLimit == 5 &&
                            !temporalEntitiesQuery.temporalQuery.hasLastN()
                    },
                    any()
                )
            }
        }

    @Test
    fun `query temporal entity should limit to temporalLimit if lastN greater than temporalLimit`() =
        runTest {
            setupTemporalPaginationWithLastNTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "lastN=7&" +
                        "type=BeeHive&options=temporalValues"
                )
                .exchange()
                .expectStatus().isEqualTo(206)

            coVerify {
                queryService.queryTemporalEntities(
                    match { temporalEntitiesQuery ->
                        temporalEntitiesQuery.temporalQuery.instanceLimit == 5 &&
                            temporalEntitiesQuery.temporalQuery.lastN == 7
                    },
                    any()
                )
            }
        }

    @Test
    fun `query temporal entity should return 200 if there is no truncated attributes`() =
        runTest {
            setupTemporalTestWithNonTruncatedValue()

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
    fun `query temporal entity should return 200 if there is truncated attribute to lastN`() =
        runTest {
            setupTemporalPaginationWithLastNTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "lastN=5&" +
                        "type=BeeHive&options=temporalValues"
                )
                .exchange()
                .expectStatus().isEqualTo(200)
        }

    @Test
    fun `query temporal entity should return 206 if there is truncated attributes`() =
        runTest {
            setupTemporalPaginationTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader().valueMatches("Content-Range", ".*/\\*")
        }

    @Test
    fun `query temporal entity with timerel between should return range-start = timeAt`() =
        runTest {
            setupTemporalPaginationTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=between&timeAt=$timeAt&endTimeAt=$endTimeAt&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $timeAt-$mostRecentTimestamp/*"
                )
        }

    @Test
    fun `query temporal entity with timerel after should return range-start = timeAt`() =
        runTest {
            setupTemporalPaginationTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $timeAt-$mostRecentTimestamp/*"
                )
        }

    @Test
    fun `query temporal entity with timerel before should return range-start = least recent timestamp`() =
        runTest {
            setupTemporalPaginationTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=before&timeAt=$timeAt&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $leastRecentTimestamp-$mostRecentTimestamp/*"
                )
        }

    @Test
    fun `query temporal entity with lastN and timerel between should return range-start = endTimeAt`() =
        runTest {
            setupTemporalPaginationWithLastNTest()
            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=between&timeAt=$timeAt&endTimeAt=$endTimeAt&" +
                        "lastN=100&" +
                        "type=BeeHive&options=temporalValues"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $endTimeAt-$leastRecentTimestamp/*"
                )
        }

    @Test
    fun `query temporal entity with lastN and timerel after should return range-start = most recent timestamp`() =
        runTest {
            setupTemporalPaginationWithLastNTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "lastN=100&" +
                        "type=BeeHive&options=temporalValues"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $mostRecentTimestamp-$leastRecentTimestamp/*"
                )
        }

    @Test
    fun `query temporal entity with lastN and timerel before should return range-start = timeAt`() =
        runTest {
            setupTemporalPaginationWithLastNTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=before&timeAt=$timeAt&" +
                        "lastN=100&" +
                        "type=BeeHive&options=temporalValues"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $timeAt-$leastRecentTimestamp/*"
                )
        }

    @Test
    fun `query temporal entity with temporalValues should return filtered data`() =
        runTest {
            setupTemporalPaginationTestwithTemporalValues()
            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "type=BeeHive&options=temporalValues"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $timeAt-$mostRecentTimestamp/*"
                ).expectBody()
                .json(loadSampleData("temporal/pagination/expected_temporal_value_filtered.json"))
        }

    @Test
    fun `query temporal entity without temporalValues option should return filtered Data`() =
        runTest {
            setupTemporalPaginationTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=after&timeAt=$timeAt&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "date-time $timeAt-$mostRecentTimestamp/*"
                ).expectBody().json(loadSampleData("temporal/pagination/expected_without_temporal_value_filtered.json"))
        }
}
