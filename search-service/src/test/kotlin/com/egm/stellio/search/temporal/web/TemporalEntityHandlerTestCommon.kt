package com.egm.stellio.search.temporal.web

import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.model.NGSILDWarning
import com.egm.stellio.search.csr.service.DistributedTemporalEntityConsumptionService
import com.egm.stellio.search.temporal.service.TemporalQueryService
import com.egm.stellio.search.temporal.service.TemporalService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@WebFluxTest(TemporalEntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
open class TemporalEntityHandlerTestCommon {
    @Autowired
    protected lateinit var webClient: WebTestClient

    @MockkBean
    protected lateinit var temporalService: TemporalService

    @MockkBean(relaxed = true)
    protected lateinit var temporalQueryService: TemporalQueryService

    @MockkBean
    protected lateinit var distributedTemporalEntityConsumptionService: DistributedTemporalEntityConsumptionService

    @BeforeEach
    fun mockNoCSR() {
        coEvery {
            distributedTemporalEntityConsumptionService.distributeRetrieveTemporalEntityOperation(
                any(), any(), any(), any()
            )
        } returns (emptyList<NGSILDWarning>() to emptyList())
        coEvery {
            distributedTemporalEntityConsumptionService.distributeQueryTemporalEntitiesOperation(
                any(), any(), any()
            )
        } returns Triple(emptyList(), emptyList(), emptyList())
    }

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }
}
