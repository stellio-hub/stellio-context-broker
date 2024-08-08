package com.egm.stellio.search.temporal.web

import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.service.EntityAttributeService
import com.egm.stellio.search.entity.service.EntityPayloadService
import com.egm.stellio.search.temporal.service.AttributeInstanceService
import com.egm.stellio.search.temporal.service.TemporalQueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.ninjasquad.springmockk.MockkBean
import org.junit.jupiter.api.BeforeAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
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

    @MockkBean(relaxed = true)
    protected lateinit var temporalQueryService: TemporalQueryService

    @MockkBean
    protected lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    protected lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean(relaxed = true)
    protected lateinit var entityAttributeService: EntityAttributeService

    @MockkBean
    protected lateinit var authorizationService: AuthorizationService

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
