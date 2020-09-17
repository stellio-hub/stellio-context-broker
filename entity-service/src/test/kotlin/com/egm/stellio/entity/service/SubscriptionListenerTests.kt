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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [SubscriptionListener::class])
@ActiveProfiles("test")
class SubscriptionListenerTests {

    @Autowired
    private lateinit var subscriptionListener: SubscriptionListener

    @MockkBean
    private lateinit var subscriptionHandlerService: SubscriptionHandlerService

    @Test
    fun `it should parse and create subscription entity`() {
        val subscription =
            loadSampleData("subscriptionEvents/subscription_event.jsonld")

        every { subscriptionHandlerService.createSubscriptionEntity(any(), any(), any()) } just Runs

        subscriptionListener.processSubscription(subscription)

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
    fun `it should parse and create notification entity`() {
        val notification =
            loadSampleData("subscriptionEvents/notification_event.jsonld")

        every { subscriptionHandlerService.createNotificationEntity(any(), any(), any(), any()) } just Runs

        subscriptionListener.processNotification(notification)

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
