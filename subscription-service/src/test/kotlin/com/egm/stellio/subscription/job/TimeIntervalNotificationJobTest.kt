package com.egm.stellio.subscription.job

import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.config.ApplicationProperties.TenantConfiguration
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.subscription.config.WebClientConfig
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.NotificationTrigger.ATTRIBUTE_CREATED
import com.egm.stellio.subscription.model.NotificationTrigger.ENTITY_CREATED
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.CoreAPIService
import com.egm.stellio.subscription.service.NotificationService
import com.egm.stellio.subscription.service.SubscriptionService
import com.egm.stellio.subscription.support.gimmeRawSubscription
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockkClass
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
    private lateinit var coreAPIService: CoreAPIService

    @MockkBean
    private lateinit var applicationProperties: ApplicationProperties

    @Test
    fun `it should compose the query string used to get matching entities`() {
        val entities = setOf(
            EntitySelector(
                id = "urn:ngsi-ld:FishContainment:1234567890".toUri(),
                idPattern = null,
                typeSelection = "FishContainment"
            ),
            EntitySelector(
                id = null,
                idPattern = null,
                typeSelection = "FishContainment"
            ),
            EntitySelector(
                id = null,
                idPattern = ".*FishContainment.*",
                typeSelection = "FishContainment"
            ),
            EntitySelector(
                id = "urn:ngsi-ld:FishContainment:1234567890".toUri(),
                idPattern = ".*FishContainment.*",
                typeSelection = "https://uri.fiware.org/ns/data-models#FishContainment"
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
    fun `it should not return twice the same entity if there is overlap between 2 entity infos`() {
        val subscription = gimmeRawSubscription(withEndpointReceiverInfo = false).copy(
            entities = setOf(
                EntitySelector(
                    id = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                    idPattern = null,
                    typeSelection = "BeeHive"
                ),
                EntitySelector(id = null, idPattern = null, typeSelection = "BeeHive")
            ),
            q = null
        )

        coEvery {
            coreAPIService.getEntities(any(), any(), any())
        } returnsMany listOf(
            listOf(loadSampleData("beehive.jsonld").deserializeAsMap()),
            listOf(
                loadSampleData("beehive.jsonld").deserializeAsMap(),
                loadSampleData("beehive2.jsonld").deserializeAsMap()
            )
        )

        runBlocking {
            val compactedEntities = timeIntervalNotificationJob.getEntitiesToNotify(
                DEFAULT_TENANT_NAME,
                subscription,
                APIC_HEADER_LINK
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
        val subscription = mockkClass(Subscription::class) {
            every { notificationTrigger } returns listOf(ENTITY_CREATED.notificationTrigger)
        }

        coEvery {
            notificationService.callSubscriber(any(), any(), any())
        } returns Triple(subscription, mockkClass(Notification::class), true)

        timeIntervalNotificationJob.sendNotification(compactedEntity, subscription)

        coVerify(exactly = 1) {
            notificationService.callSubscriber(subscription, ENTITY_CREATED, listOf(compactedEntity))
        }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should notify the recurring subscriptions that have reached the time interval`() = runTest {
        val entity = loadSampleData("beehive.jsonld")
        val subscription = gimmeRawSubscription(withEndpointReceiverInfo = false).copy(
            entities = setOf(
                EntitySelector(
                    id = null,
                    idPattern = null,
                    typeSelection = "https://uri.fiware.org/ns/data-models#BeeHive"
                )
            )
        )

        every {
            applicationProperties.tenants
        } returns listOf(TenantConfiguration(DEFAULT_TENANT_NAME, "", "public"))
        coEvery { subscriptionService.getRecurringSubscriptionsToNotify() } returns listOf(subscription)
        coEvery { subscriptionService.getContextsLink(any()) } returns NGSILD_TEST_CORE_CONTEXT
        coEvery { coreAPIService.getEntities(any(), any(), any()) } returns listOf(entity.deserializeAsMap())
        coEvery {
            notificationService.callSubscriber(any(), any(), any())
        } returns Triple(subscription, mockkClass(Notification::class), true)

        timeIntervalNotificationJob.sendTimeIntervalNotification()

        coVerify(exactly = 1, timeout = 1000L) {
            notificationService.callSubscriber(subscription, ATTRIBUTE_CREATED, listOf(entity.deserializeAsMap()))
        }
        confirmVerified(notificationService)
    }
}
