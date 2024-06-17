package com.egm.stellio.search.web

import arrow.core.Either
import com.egm.stellio.search.config.SearchProperties
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.loadAndExpandSampleData
import io.mockk.coEvery
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class TemporalEntityHandlerpaginationTests : TemporalEntityHandlerTests() {

    private suspend fun setupSize5TemporalTest() {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/pagination/beehive_with_five_unsynchronized_temporal_attributes_evolution.jsonld"
        )
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2))
    }

    private suspend fun setupSize10TemporalTestwithTemporalValues() {
        val firstTemporalEntity = loadAndExpandSampleData(
            "/temporal/pagination/beehive_with_ten_unsynchronized_temporal_attributes_evolution_temporal_values.jsonld"
        )
        val secondTemporalEntity = loadAndExpandSampleData("beehive.jsonld")

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery {
            queryService.queryTemporalEntities(any(), any())
        } returns Either.Right(Pair(listOf(firstTemporalEntity, secondTemporalEntity), 2))
    }

    @Test
    fun `query temporal entity should return 206 if there is truncated temporal attributes`() = runTest {
        setupSize5TemporalTest()

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                    "lastN=5&" +
                    "type=BeeHive"
            )
            .exchange()
            .expectStatus().isEqualTo(206)
            .expectHeader().valueMatches("Content-Range", ".*/5")
    }


    @Test
    fun `query temporal entity without lastN and timerel before should return 206 if there is truncated attributes`() =
        runTest {
            setupSize5TemporalTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader().valueMatches("Content-Range", ".*/\\*")
        }

    @Test
    fun `query temporal entity without lastN and with timerel between should return range-start = timeAt`() =
        runTest {
            setupSize5TemporalTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=between&timeAt=2019-10-17T07:31:39Z&endTimeAt=2019-10-18T07:31:39Z&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "DateTime Thu, 17 Oct 2019 07:31:39 GMT-Fri, 24 Jan 2020 13:05:22 GMT/*"
                )
        }

    @Test
    fun `query temporal entity without lastN and with timerel after should return range-start = timeAt`() =
        runTest {
            setupSize5TemporalTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=before&timeAt=2019-10-17T07:31:39Z&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "DateTime Thu, 17 Oct 2019 07:31:39 GMT-Fri, 24 Jan 2020 13:05:22 GMT/*"
                )
        }

    @Test
    fun `query temporal entity with timerel after should return range-start = least recent timestamp`() =
        runTest {
            setupSize5TemporalTest()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=before&timeAt=2019-10-17T07:31:39Z&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "DateTime Fri, 24 Jan 2020 13:01:22 GMT-Fri, 24 Jan 2020 13:05:22 GMT/*"
                )
        }

    @Test
    fun `query temporal entity uqdyègèfgq`() =
        runTest {
            setupSize10TemporalTestwithTemporalValues()

            webClient.get()
                .uri(
                    "/ngsi-ld/v1/temporal/entities?" +
                        "timerel=before&timeAt=2019-10-17T07:31:39Z&" +
                        "lastN=10&" +
                        "type=BeeHive"
                )
                .exchange()
                .expectStatus().isEqualTo(206)
                .expectHeader()
                .valueEquals(
                    "Content-Range",
                    "DateTime Fri, 24 Jan 2020 13:01:22 GMT-Fri, 24 Jan 2020 13:05:22 GMT/*"
                )
        }
}

