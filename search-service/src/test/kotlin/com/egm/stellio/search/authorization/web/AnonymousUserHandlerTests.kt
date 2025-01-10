package com.egm.stellio.search.authorization.web

import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.csr.service.ContextSourceCaller
import com.egm.stellio.search.entity.service.EntityEventService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.entity.service.LinkedEntityService
import com.egm.stellio.search.entity.web.EntityHandler
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.util.AQUAC_HEADER_LINK
import com.ninjasquad.springmockk.MockkBean
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.http.HttpHeaders
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient

@ActiveProfiles("test")
@WebFluxTest(EntityHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
@Suppress("unused")
class AnonymousUserHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @MockkBean
    private lateinit var entityEventService: EntityEventService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var contextSourceCaller: ContextSourceCaller

    @MockkBean(relaxed = true)
    private lateinit var linkedEntityService: LinkedEntityService

    @Test
    @WithAnonymousUser
    fun `it should not authorize an anonymous to call the API`() {
        webClient
            .get()
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:Sensor:0022CCC")
            .header(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            .exchange()
            .expectStatus().isUnauthorized
    }
}
