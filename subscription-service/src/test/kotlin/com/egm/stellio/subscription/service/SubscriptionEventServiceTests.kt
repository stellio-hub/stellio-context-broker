package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(classes = [SubscriptionEventService::class])
@ActiveProfiles("test")
class SubscriptionEventServiceTests {

    @Autowired
    private lateinit var subscriptionEventService: SubscriptionEventService

    @MockkBean(relaxed = true)
    private lateinit var resolver: BinderAwareChannelResolver

    @Test
    fun `it should publish an event of type SUBSCRIPTION_CREATE`() {
        val event = SubscriptionCreateEvent("urn:ngsi-ld:Subscription:1".toUri(), "operationPayload")
        every {
            resolver.resolveDestination(any()).send(any())
        } returns true

        subscriptionEventService.publishSubscriptionEvent(event)

        verify { resolver.resolveDestination("cim.subscription") }
    }

    @Test
    fun `it should publish an event of type NOTIFICATION_CREATE`() {
        val event = NotificationCreateEvent("urn:ngsi-ld:Notification:1".toUri(), "operationPayload")
        every {
            resolver.resolveDestination(any()).send(any())
        } returns true

        subscriptionEventService.publishNotificationEvent(event)

        verify { resolver.resolveDestination("cim.notification") }
    }
}
