package com.egm.stellio.entity.service

import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Runs
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.just
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(classes = [SubscriptionEventListenerService::class])
@ActiveProfiles("test")
class SubscriptionEventListenerServiceTests {

    @Autowired
    private lateinit var subscriptionEventListenerService: SubscriptionEventListenerService

    @MockkBean
    private lateinit var subscriptionHandlerService: SubscriptionHandlerService

    @Test
    fun `it should parse and create subscription entity`() {
        val subscription =
            loadSampleData("events/listened/subscriptionCreateEvent.jsonld")

        every { subscriptionHandlerService.createSubscriptionEntity(any(), any(), any()) } just Runs

        subscriptionEventListenerService.processSubscription(subscription)

        verify {
            subscriptionHandlerService.createSubscriptionEntity(
                "urn:ngsi-ld:Subscription:04".toUri(),
                "Subscription",
                mapOf("q" to "foodQuantity<150;foodName=='dietary fibres'")
            )
        }
        confirmVerified(subscriptionHandlerService)
    }

    @Test
    fun `it should delete a subscription entity`() {
        val subscriptionEvent =
            loadSampleData("events/listened/subscriptionDeleteEvent.jsonld")

        every { subscriptionHandlerService.deleteSubscriptionEntity(any()) } returns Pair(2, 1)

        subscriptionEventListenerService.processSubscription(subscriptionEvent)

        verify {
            subscriptionHandlerService.deleteSubscriptionEntity(
                "urn:ngsi-ld:Subscription:04".toUri()
            )
        }

        confirmVerified(subscriptionHandlerService)
    }

    @Test
    fun `it should parse and create notification entity`() {
        val notification =
            loadSampleData("events/listened/notificationCreateEvent.jsonld")

        every { subscriptionHandlerService.createNotificationEntity(any(), any(), any(), any()) } just Runs

        subscriptionEventListenerService.processNotification(notification)

        verify {
            subscriptionHandlerService.createNotificationEntity(
                "urn:ngsi-ld:Notification:1234".toUri(),
                "Notification",
                "urn:ngsi-ld:Subscription:1234".toUri(),
                mapOf("notifiedAt" to "2020-03-10T00:00:00Z")
            )
        }
        confirmVerified(subscriptionHandlerService)
    }
}
