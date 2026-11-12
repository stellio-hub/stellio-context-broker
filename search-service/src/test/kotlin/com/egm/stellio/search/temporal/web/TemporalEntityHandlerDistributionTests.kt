package com.egm.stellio.search.temporal.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.DistributedTemporalQueryResult
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.temporal.model.TemporalPagination
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ErrorMessages.Entity.entityNotFoundMessage
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.toUri
import io.mockk.coEvery
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class TemporalEntityHandlerDistributionTests : TemporalEntityHandlerTestCommon() {

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    private val remoteEntity = mapOf(
        "id" to entityUri.toString(),
        "type" to "BeeHive",
        "temperature" to mapOf("type" to "Property", "value" to 22.5),
        "@context" to listOf(NGSILD_TEST_CORE_CONTEXT)
    )

    private fun csrPagination(parameter: String) = TemporalPagination(
        offsetBefore = if (parameter == "lastN") 7 else 1,
        offsetAfter = if (parameter == "lastN") 1 else 7
    )

    @ParameterizedTest
    @ValueSource(strings = ["firstN", "lastN"])
    fun `getForEntity should use CSR offsets when local history is missing`(parameter: String) = runTest {
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        coEvery {
            distributedTemporalEntityConsumptionService.distributeRetrieveTemporalEntityOperation(
                any(), any(), any(), any()
            )
        } returns DistributedTemporalQueryResult(
            entities = listOf(remoteEntity to gimmeRawCSR()),
            count = 1,
            pagination = csrPagination(parameter)
        )

        val path = "/ngsi-ld/v1/temporal/entities/$entityUri"
        webClient.get()
            .uri("$path?$parameter=10&offsetN=4")
            .exchange()
            .expectStatus().isOk
            .expectHeader().values(HttpHeaders.LINK) { links ->
                assertCsrOffsets(links, path, parameter)
            }
    }

    @ParameterizedTest
    @ValueSource(strings = ["firstN", "lastN"])
    fun `getForEntities should use CSR offsets when local history is empty`(parameter: String) = runTest {
        coEvery { temporalQueryService.queryTemporalEntities(any()) } returns
            Triple(emptyList<ExpandedEntity>(), 0, false).right()
        coEvery {
            distributedTemporalEntityConsumptionService.distributeQueryTemporalEntitiesOperation(any(), any(), any())
        } returns DistributedTemporalQueryResult(
            entities = listOf(listOf(remoteEntity) to gimmeRawCSR()),
            pagination = csrPagination(parameter)
        )

        val path = "/ngsi-ld/v1/temporal/entities"
        webClient.get()
            .uri(
                "$path?type=BeeHive&timerel=after&timeAt=2019-10-17T07:31:39Z&" +
                    "$parameter=10&offsetN=4"
            )
            .exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$[0].id").isEqualTo(entityUri.toString())
            .returnResult()
            .responseHeaders[HttpHeaders.LINK]!!.let { assertCsrOffsets(it, path, parameter) }
    }

    private fun assertCsrOffsets(links: List<String>, path: String, parameter: String) {
        val afterOffset = if (parameter == "firstN") 7 else 1
        val beforeOffset = if (parameter == "firstN") 1 else 7
        assertThat(links).anyMatch {
            it.startsWith("<$path?") && it.contains("offsetN=$afterOffset>") && it.contains("rel=\"intervalafter\"")
        }.anyMatch {
            it.startsWith("<$path?") && it.contains("offsetN=$beforeOffset>") && it.contains("rel=\"intervalbefore\"")
        }
    }

    @Test
    fun `query temporal entity should return 404 when not found locally nor in any remote CSR`() {
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        coEvery {
            distributedTemporalEntityConsumptionService.distributeRetrieveTemporalEntityOperation(
                any(), any(), any(), any()
            )
        } returns DistributedTemporalQueryResult()

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `query temporal entities should surface NGSILD-Warning headers on distributed warnings`() = runTest {
        val csr = gimmeRawCSR()
        val warning = MiscellaneousWarning("CSR unavailable", csr)

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Triple(emptyList<ExpandedEntity>(), 0, false).right()

        coEvery {
            distributedTemporalEntityConsumptionService.distributeQueryTemporalEntitiesOperation(
                any(), any(), any()
            )
        } returns DistributedTemporalQueryResult(warnings = listOf(warning))

        webClient.get()
            .uri(
                "/ngsi-ld/v1/temporal/entities?" +
                    "type=BeeHive&timerel=after&timeAt=2019-10-17T07:31:39Z"
            )
            .exchange()
            .expectStatus().isOk
            .expectHeader().exists(NGSILDWarning.HEADER_NAME)
    }
}
