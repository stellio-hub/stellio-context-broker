package com.egm.stellio.search.temporal.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ErrorMessages.Entity.entityNotFoundMessage
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.toUri
import io.mockk.coEvery
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class TemporalEntityHandlerDistributionTests : TemporalEntityHandlerTestCommon() {

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should return 200 when entity is not found locally but found in a remote CSR`() = runTest {
        val csr = gimmeRawCSR()
        val remoteEntity = mapOf(
            "id" to entityUri.toString(),
            "type" to "BeeHive",
            "temperature" to mapOf("type" to "Property", "value" to 22.5),
            "@context" to listOf(NGSILD_TEST_CORE_CONTEXT)
        )

        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        coEvery {
            distributedTemporalEntityConsumptionService.distributeRetrieveTemporalEntityOperation(
                any(), any(), any(), any()
            )
        } returns (emptyList<NGSILDWarning>() to listOf(remoteEntity to csr))

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `it should return 404 when entity is not found locally nor in any remote CSR`() {
        coEvery {
            temporalQueryService.queryTemporalEntity(any(), any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        coEvery {
            distributedTemporalEntityConsumptionService.distributeRetrieveTemporalEntityOperation(
                any(), any(), any(), any()
            )
        } returns (emptyList<NGSILDWarning>() to emptyList())

        webClient.get()
            .uri("/ngsi-ld/v1/temporal/entities/$entityUri")
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    fun `it should surface NGSILD-Warning headers when distributed query raises warnings`() = runTest {
        val csr = gimmeRawCSR()
        val warning = MiscellaneousWarning("CSR unavailable", csr)

        coEvery {
            temporalQueryService.queryTemporalEntities(any())
        } returns Triple(emptyList<ExpandedEntity>(), 0, null).right()

        coEvery {
            distributedTemporalEntityConsumptionService.distributeQueryTemporalEntitiesOperation(
                any(), any(), any()
            )
        } returns Triple(listOf(warning), emptyList(), emptyList())

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
