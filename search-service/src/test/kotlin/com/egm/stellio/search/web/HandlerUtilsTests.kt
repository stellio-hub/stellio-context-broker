package com.egm.stellio.search.web

import com.egm.stellio.search.config.WebSecurityTestConfig
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.TemporalEntityService
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.context.annotation.Import
import org.springframework.http.MediaType
import org.springframework.security.test.context.support.WithAnonymousUser
import org.springframework.security.test.context.support.WithMockUser
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.UUID

@ActiveProfiles("test")
@WebFluxTest(HandlerUtils::class)
class HandlerUtilsTests {

    @Autowired
    private lateinit var handlerUtils: HandlerUtils

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityService: TemporalEntityService

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    @Test
    suspend fun `it should raise a 400 for query temporal entities if neither type nor attrs is present in query params`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "outgoing")

        val temporalEntities = handlerUtils.queryTemporalEntities(queryParams, apicContext!!)

    }

    }
