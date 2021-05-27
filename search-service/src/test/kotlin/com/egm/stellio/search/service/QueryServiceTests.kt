package com.egm.stellio.search.service

import com.egm.stellio.search.config.CoroutineTestRule
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.configureFor
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.every
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Rule
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class QueryServiceTests {

    @get:Rule
    var coroutinesTestRule = CoroutineTestRule()

    @Autowired
    private lateinit var queryService: QueryService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var temporalEntityService: TemporalEntityService


    @Test
    fun `it throw an error`() = coroutinesTestRule.testDispatcher.runBlockingTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "between")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("endTime", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive")

        val firstTemporalEntity = deserializeObject(
            loadSampleData("beehive_with_two_temporal_attributes_evolution.jsonld")
        ).minus("@context")
        val secondTemporalEntity = deserializeObject(loadSampleData("beehive.jsonld")).minus("@context")
        every { temporalEntityAttributeService.getForEntities(any(), any(), any()) } returns Mono.just(emptyMap())
        every { attributeInstanceService.search(any(), any(), any()) } returns Mono.just(emptyList())
        every { temporalEntityService.buildTemporalEntities(any(), any(), any(), any()) } returns
            listOf(firstTemporalEntity, secondTemporalEntity)


        queryService.queryTemporalEntities(queryParams, APIC_COMPOUND_CONTEXT)
    }
}
