package com.egm.stellio.search.web

import arrow.core.Either
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.loadAndExpandSampleData
import com.egm.stellio.shared.util.toNgsiLdFormat
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.time.ZonedDateTime

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(SearchProperties::class)
@TestPropertySource(properties = ["application.pagination.temporal-limit = 5"])
class TemporalEntityHandlerPaginationTests : TemporalEntityHandlerTestCommon() {

    private val timeAt = "2019-01-01T00:00:00Z"
    private val endTimeAt = "2021-01-01T00:00:00Z"
    private val range = ZonedDateTime.parse(timeAt) to ZonedDateTime.parse(endTimeAt)
    private val lastN = "100"

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
        mockService(firstTemporalEntity, range)
    }

    private suspend fun setupTemporalPaginationWithLastNTest() {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/pagination/beehive_with_five_temporal_attributes_evolution_temporal_values_and_lastN.jsonld"
        )
        mockService(firstTemporalEntity, range)
    }

    private suspend fun mockService(firstTemporalEntity: ExpandedEntity, range: Range? = null) {
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")
        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(
            Triple(
                listOf(firstTemporalEntity, secondTemporalEntity),
                2,
                range
            )
        )
    }

    @Test
    fun `query temporal entity should limit to temporal pagination limit`() = runTest {
        setupTemporalTestWithNonTruncatedValue()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "type=BeeHive"
            )
            .exchange()

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
    fun `query temporal entity should limit to temporal limit if lastN greater than temporal limit`() = runTest {
        setupTemporalPaginationTest()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "lastN=7&" +
                    "type=BeeHive"
            )
            .exchange()

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
    fun `query temporal entity should return 200 if pagination was not triggered`() = runTest {
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
    fun `query temporal entity should return 206 if pagination was triggered`() = runTest {
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
    fun `query temporal entity with lastN should return well formed Content-Range header`() = runTest {
        setupTemporalPaginationWithLastNTest()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "lastN=$lastN&" +
                    "type=BeeHive"
            )
            .exchange()
            .expectStatus().isEqualTo(206)
            .expectHeader()
            .valueEquals(
                "Content-Range",
                "date-time ${range.first.toNgsiLdFormat()}-${range.second.toNgsiLdFormat()}/$lastN"
            )
    }
}
