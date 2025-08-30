package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.config.WebClientConfig
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.Endpoint.AcceptType
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import com.egm.stellio.subscription.model.NotificationParams.JoinType
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.okJson
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [CoreAPIService::class])
@WireMockTest(httpPort = 8089)
@Import(WebClientConfig::class)
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class CoreAPIServiceTests {

    @Autowired
    private lateinit var coreApiService: CoreAPIService

    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should return one entity if the query sent to entity service matches one entity`() {
        val encodedQuery = "?type=BeeHive&id=urn:ngsi-ld:BeeHive:TESTC&q=speed%3E50%3BfoodName%3D%3Ddietary+fibres"
        stubFor(
            get(urlEqualTo("/ngsi-ld/v1/entities$encodedQuery"))
                .willReturn(
                    okJson(
                        listOf(
                            loadSampleData("beehive.jsonld")
                        ).toString()
                    )
                )
        )

        val query = "?type=BeeHive&id=urn:ngsi-ld:BeeHive:TESTC&q=speed%3E50%3BfoodName%3D%3Ddietary+fibres"
        runBlocking {
            val compactedEntities = coreApiService.getEntities(
                DEFAULT_TENANT_NAME,
                query,
                APIC_HEADER_LINK
            )
            assertEquals(1, compactedEntities.size)
            assertEquals("urn:ngsi-ld:BeeHive:TESTC", compactedEntities[0]["id"])
        }
    }

    @Test
    fun `it should return a list of 2 entities if the query sent to entity service matches two entities`() {
        stubFor(
            get(urlEqualTo("/ngsi-ld/v1/entities?type=BeeHive"))
                .willReturn(
                    okJson(
                        listOf(
                            loadSampleData("beehive.jsonld"),
                            loadSampleData("beehive2.jsonld")
                        ).toString()
                    )
                )
        )

        runBlocking {
            val compactedEntities = coreApiService.getEntities(
                DEFAULT_TENANT_NAME,
                "?type=BeeHive",
                APIC_HEADER_LINK
            )
            assertEquals(2, compactedEntities.size)
            assertEquals("urn:ngsi-ld:BeeHive:TESTC", compactedEntities[0]["id"])
            assertEquals("urn:ngsi-ld:BeeHive:TESTD", compactedEntities[1]["id"])
        }
    }

    @Test
    fun `it should ask to retrieve linked entitiies using the notification parameters from the subscription`() {
        stubFor(
            get(urlPathEqualTo("/ngsi-ld/v1/entities/$beehiveTestCId"))
                .willReturn(
                    okJson(
                        listOf(
                            loadSampleData("beehive.jsonld"),
                            loadSampleData("beehive2.jsonld")
                        ).toString()
                    )
                )
        )

        runBlocking {
            val compactedEntities = coreApiService.retrieveLinkedEntities(
                DEFAULT_TENANT_NAME,
                beehiveTestCId,
                NotificationParams(
                    attributes = listOf(INCOMING_TERM),
                    format = FormatType.NORMALIZED,
                    endpoint = Endpoint(
                        uri = "http://localhost:8089/notification".toUri(),
                        accept = AcceptType.JSONLD
                    ),
                    join = JoinType.FLAT,
                    joinLevel = 1,
                    pick = setOf(JSONLD_ID_KW, JSONLD_TYPE_KW, INCOMING_TERM)
                ),
                APIC_HEADER_LINK
            )
            assertEquals(2, compactedEntities.size)
            assertEquals("urn:ngsi-ld:BeeHive:TESTC", compactedEntities[0]["id"])
            assertEquals("urn:ngsi-ld:BeeHive:TESTD", compactedEntities[1]["id"])
        }

        verify(
            getRequestedFor(urlPathEqualTo("/ngsi-ld/v1/entities/$beehiveTestCId"))
                .withHeader(HttpHeaders.LINK, equalTo(APIC_HEADER_LINK))
                .withHeader(HttpHeaders.ACCEPT, equalTo(AcceptType.JSONLD.accept))
                .withHeader(NGSILD_TENANT_HEADER, equalTo(DEFAULT_TENANT_NAME))
                .withQueryParam(QueryParameter.JOIN.key, equalTo(JoinType.FLAT.join))
                .withQueryParam(QueryParameter.JOIN_LEVEL.key, equalTo("1"))
                .withQueryParam(QueryParameter.OPTIONS.key, equalTo(FormatType.NORMALIZED.format))
                .withQueryParam(QueryParameter.ATTRS.key, equalTo(INCOMING_TERM))
                .withQueryParam(QueryParameter.PICK.key, equalTo("$JSONLD_ID_KW,$JSONLD_TYPE_KW,$INCOMING_TERM"))
        )
    }
}
