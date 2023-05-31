package com.egm.stellio.subscription.job

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.config.ApplicationProperties.TenantConfiguration
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.web.DEFAULT_TENANT_URI
import com.egm.stellio.subscription.config.WebClientConfig
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.NotificationService
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [TimeIntervalNotificationJob::class])
@WireMockTest(httpPort = 8089)
@Import(WebClientConfig::class)
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class TimeIntervalNotificationJobTest {

    @Autowired
    private lateinit var timeIntervalNotificationJob: TimeIntervalNotificationJob

    @MockkBean
    private lateinit var notificationService: NotificationService

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @MockkBean
    private lateinit var applicationProperties: ApplicationProperties

    @Test
    fun `it should compose the query string used to get matching entities`() {
        val entities = setOf(
            EntityInfo(
                id = "urn:ngsi-ld:FishContainment:1234567890".toUri(),
                idPattern = null,
                type = "FishContainment"
            ),
            EntityInfo(
                id = null,
                idPattern = null,
                type = "FishContainment"
            ),
            EntityInfo(
                id = null,
                idPattern = ".*FishContainment.*",
                type = "FishContainment"
            ),
            EntityInfo(
                id = "urn:ngsi-ld:FishContainment:1234567890".toUri(),
                idPattern = ".*FishContainment.*",
                type = "https://uri.fiware.org/ns/data-models#FishContainment"
            )
        )
        val q = "speed>50;foodName==dietary fibres"

        assertEquals(
            "?type=FishContainment" +
                "&id=urn:ngsi-ld:FishContainment:1234567890" +
                "&q=speed%3E50%3BfoodName%3D%3Ddietary+fibres",
            timeIntervalNotificationJob.prepareQueryParams(entities.elementAt(0), q, null)
        )
        assertEquals(
            "?type=FishContainment" +
                "&q=speed%3E50%3BfoodName%3D%3Ddietary+fibres",
            timeIntervalNotificationJob.prepareQueryParams(entities.elementAt(1), q, null)
        )
        assertEquals(
            "?type=FishContainment" +
                "&idPattern=.*FishContainment.*" +
                "&q=speed%3E50%3BfoodName%3D%3Ddietary+fibres",
            timeIntervalNotificationJob.prepareQueryParams(entities.elementAt(2), q, null)
        )
        assertEquals(
            "?type=https%3A%2F%2Furi.fiware.org%2Fns%2Fdata-models%23FishContainment" +
                "&id=urn:ngsi-ld:FishContainment:1234567890" +
                "&idPattern=.*FishContainment.*" +
                "&q=speed%3E50%3BfoodName%3D%3Ddietary+fibres",
            timeIntervalNotificationJob.prepareQueryParams(entities.elementAt(3), q, null)
        )
    }

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
            val compactedEntities = timeIntervalNotificationJob.getEntities(
                DEFAULT_TENANT_URI,
                query,
                buildContextLinkHeader(APIC_COMPOUND_CONTEXT)
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
            val compactedEntities = timeIntervalNotificationJob.getEntities(
                DEFAULT_TENANT_URI,
                "?type=BeeHive",
                buildContextLinkHeader(APIC_COMPOUND_CONTEXT)
            )
            assertEquals(2, compactedEntities.size)
            assertEquals("urn:ngsi-ld:BeeHive:TESTC", compactedEntities[0]["id"])
            assertEquals("urn:ngsi-ld:BeeHive:TESTD", compactedEntities[1]["id"])
        }
    }

    @Test
    fun `it should not return twice the same entity if there is overlap between 2 entity infos`() {
        val subscription = gimmeRawSubscription(withEndpointInfo = false).copy(
            entities = setOf(
                EntityInfo(
                    id = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                    idPattern = null,
                    type = "BeeHive"
                ),
                EntityInfo(id = null, idPattern = null, type = "BeeHive")
            ),
            q = null
        )

        stubFor(
            get(urlEqualTo("/ngsi-ld/v1/entities?type=BeeHive&id=urn:ngsi-ld:BeeHive:TESTC"))
                .willReturn(
                    okJson(
                        listOf(
                            loadSampleData("beehive.jsonld")
                        ).toString()
                    )
                )
        )

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
            val compactedEntities = timeIntervalNotificationJob.getEntitiesToNotify(
                DEFAULT_TENANT_URI,
                subscription,
                buildContextLinkHeader(APIC_COMPOUND_CONTEXT)
            )
            assertEquals(2, compactedEntities.size)
            assertTrue(
                compactedEntities.all {
                    it["id"] == "urn:ngsi-ld:BeeHive:TESTC" || it["id"] == "urn:ngsi-ld:BeeHive:TESTD"
                }
            )
        }
    }

    @Test
    fun `it should call 'notificationService' once`() = runTest {
        val compactedEntity = mapOf("id" to "urn:ngsi-ld:Entity:01")
        val subscription = mockkClass(Subscription::class)

        coEvery {
            notificationService.callSubscriber(any(), any())
        } returns Triple(subscription, mockkClass(Notification::class), true)

        timeIntervalNotificationJob.sendNotification(compactedEntity, subscription)

        coVerify(exactly = 1) { notificationService.callSubscriber(subscription, compactedEntity) }
        confirmVerified()
    }

    @Test
    fun `it should notify the recurring subscriptions that have reached the time interval`() = runTest {
        val entity = loadSampleData("beehive.jsonld")
        val subscription = gimmeRawSubscription(withEndpointInfo = false).copy(
            entities = setOf(
                EntityInfo(
                    id = null,
                    idPattern = null,
                    type = "https://uri.fiware.org/ns/data-models#BeeHive"
                )
            )
        )

        val encodedQuery = "?type=https%3A%2F%2Furi.fiware.org%2Fns%2Fdata-models%23BeeHive" +
            "&q=speed%3E50%3BfoodName%3D%3Ddietary+fibres"

        every {
            applicationProperties.tenants
        } returns listOf(TenantConfiguration(DEFAULT_TENANT_URI, "", "public"))
        coEvery { subscriptionService.getRecurringSubscriptionsToNotify() } returns listOf(subscription)
        coEvery { subscriptionService.getContextsLink(any()) } returns NGSILD_CORE_CONTEXT
        coEvery {
            notificationService.callSubscriber(any(), any())
        } returns Triple(subscription, mockkClass(Notification::class), true)

        stubFor(
            get(urlEqualTo("/ngsi-ld/v1/entities$encodedQuery"))
                .willReturn(
                    okJson(
                        listOf(
                            entity
                        ).toString()
                    )
                )
        )

        timeIntervalNotificationJob.sendTimeIntervalNotification()

        coVerify(exactly = 1, timeout = 1000L) {
            notificationService.callSubscriber(subscription, entity.deserializeAsMap())
        }
        confirmVerified(notificationService)

        verify(
            getRequestedFor(urlEqualTo("/ngsi-ld/v1/entities$encodedQuery"))
        )
    }
}
