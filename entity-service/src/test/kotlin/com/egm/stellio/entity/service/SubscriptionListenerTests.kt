package com.egm.stellio.entity.service

import com.egm.stellio.shared.util.loadSampleData
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
    private lateinit var entityService: EntityService

    @Test
    fun `it should parse and create subscription entity`() {
        val subscription =
            loadSampleData("subscriptionEvents/subscription_event.jsonld")

        every { entityService.createSubscriptionEntity(any(), any(), any()) } just Runs

        subscriptionListener.processSubscription(subscription)

        verify {
            entityService.createSubscriptionEntity(
                "urn:ngsi-ld:Subscription:04",
                "Subscription",
                mapOf("q" to "foodQuantity<150;foodName=='dietary fibres'")
            )
        }
        confirmVerified(entityService)
    }

    @Test
    fun `it should parse and create notification entity`() {
        val notification =
            loadSampleData("subscriptionEvents/notification_event.jsonld")

        every { entityService.createNotificationEntity(any(), any(), any(), any()) } just Runs

        subscriptionListener.processNotification(notification)

        verify {
            entityService.createNotificationEntity(
                "urn:ngsi-ld:Notification:1234",
                "Notification",
                "urn:ngsi-ld:Subscription:1234",
                mapOf("notifiedAt" to "2020-03-10T00:00:00Z")
            )
        }
        confirmVerified(entityService)
    }
}
