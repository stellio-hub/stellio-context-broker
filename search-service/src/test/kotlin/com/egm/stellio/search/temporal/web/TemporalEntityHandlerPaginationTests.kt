package com.egm.stellio.search.temporal.web

import arrow.core.Either
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.shared.util.DateUtils.toNgsiLdFormat
import com.egm.stellio.shared.util.loadAndExpandSampleData
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
    private val entityId = "urn:ngsi-ld:BeeHive:TESTC"

    private suspend fun mockQueryEntitiesService(range: Range? = null) {
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
                range
            )
        )
    }

    private suspend fun mockQueryEntityService(range: Range? = null) {
        val firstTemporalEntity = loadAndExpandSampleData("/temporal/beehive_create_temporal_entity.jsonld")
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns Either.Right(
            firstTemporalEntity to range
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

    @Test
    fun `query temporal entities should limit to temporal limit if lastN greater than temporal limit`() = runTest {
        mockQueryEntitiesService()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=after&timeAt=$timeAt&" +
                    "lastN=7&" +
                    "type=BeeHive"
            )
            .exchange()

        coVerify {
            temporalQueryService.queryTemporalEntities(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.instanceLimit == 5 &&
                        temporalEntitiesQuery.temporalQuery.lastN == 7
                }
            )
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
    fun `query temporal entities should return 206 if pagination was triggered`() = runTest {
        mockQueryEntitiesService(range)

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
    fun `query temporal entities with lastN should return well formed Content-Range header`() = runTest {
        mockQueryEntitiesService(range)

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

    @Test
    fun `query temporal entity should return 206 if pagination was triggered`() = runTest {
        mockQueryEntityService(range)

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities/$entityId?" +
                    "timerel=after&timeAt=$timeAt"
            )
            .exchange()
            .expectStatus().isEqualTo(206)
            .expectHeader()
            .valueEquals(
                "Content-Range",
                "date-time ${range.first.toNgsiLdFormat()}-${range.second.toNgsiLdFormat()}/*"
            )
    }
}
