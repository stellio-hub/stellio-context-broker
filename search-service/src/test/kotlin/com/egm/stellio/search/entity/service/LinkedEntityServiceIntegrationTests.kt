package com.egm.stellio.search.entity.service

import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.MiscellaneousPersistentWarning
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.search.csr.service.ContextSourceRegistrationService
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.JoinType
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.serverError
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@WireMockTest(httpPort = 8089)
@ActiveProfiles("test")
class LinkedEntityServiceIntegrationTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var linkedEntityService: LinkedEntityService

    @MockkBean
    private lateinit var contextSourceRegistrationService: ContextSourceRegistrationService

    private val remoteLinkedEntityId = "urn:ngsi-ld:RemoteLinkedEntity:01"

    private val linkingEntity: CompactedEntity = """
        {
            "id": "urn:ngsi-ld:LinkingEntity:01",
            "type": "LinkingEntity",
            "rel": {
                "type": "Relationship",
                "object": "$remoteLinkedEntityId",
                "objectType": "RemoteLinkedEntity"
            }
        }
    """.trimIndent().deserializeAsMap()

    private val inlineEntitiesQuery = EntitiesQueryFromGet(
        linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 1.toUInt()),
        paginationQuery = PaginationQuery(0, 100),
        contexts = NGSILD_TEST_CORE_CONTEXTS
    )

    @BeforeEach
    fun mockCSR() {
        coEvery {
            contextSourceRegistrationService.getContextSourceRegistrations(any(), any(), any())
        } returns listOf(
            gimmeRawCSR(
                information = listOf(
                    RegistrationInfo(listOf(EntityInfo(types = listOf("${NGSILD_DEFAULT_VOCAB}RemoteLinkedEntity"))))
                )
            )
        )
        coEvery { contextSourceRegistrationService.updateContextSourceStatus(any(), any()) } returns Unit
    }

    @Test
    fun `processLinkedEntities should inline a linked entity retrieved from a context source`() = runTest {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/entities"))
                .willReturn(
                    okJson(
                        """
                        [{
                            "id": "$remoteLinkedEntityId",
                            "type": "RemoteLinkedEntity",
                            "name": { "type": "Property", "value": "remote" }
                        }]
                        """.trimIndent()
                    )
                )
        )

        val (inlinedEntities, warnings) =
            linkedEntityService.processLinkedEntities(linkingEntity, inlineEntitiesQuery, HttpHeaders())
                .shouldSucceedAndResult()

        assertThat(warnings).isEmpty()
        val linkedEntity = (inlinedEntities.single()["rel"] as Map<*, *>)["entity"] as Map<*, *>
        assertEquals(remoteLinkedEntityId, linkedEntity["id"])
        verify(
            getRequestedFor(urlPathEqualTo("/ngsi-ld/v1/entities"))
                .withQueryParam("id", equalTo(remoteLinkedEntityId))
        )
    }

    @Test
    fun `processLinkedEntities should return a warning when the context source fails`() = runTest {
        stubFor(get(urlPathEqualTo("/ngsi-ld/v1/entities")).willReturn(serverError()))

        val (inlinedEntities, warnings) =
            linkedEntityService.processLinkedEntities(linkingEntity, inlineEntitiesQuery, HttpHeaders())
                .shouldSucceedAndResult()

        assertInstanceOf(MiscellaneousPersistentWarning::class.java, warnings.single())
        assertFalse((inlinedEntities.single()["rel"] as Map<*, *>).containsKey("entity"))
    }
}
