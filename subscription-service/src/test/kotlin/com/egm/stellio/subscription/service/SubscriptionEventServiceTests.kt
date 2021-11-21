package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.Subscription
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.concurrent.SettableListenableFuture
import reactor.core.publisher.Mono
import java.time.Instant
import java.time.ZoneOffset

@SpringBootTest(classes = [SubscriptionEventService::class])
@ActiveProfiles("test")
class SubscriptionEventServiceTests {

    @Autowired
    private lateinit var subscriptionEventService: SubscriptionEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @Test
    fun `it should publish an event of type SUBSCRIPTION_CREATE`() {
        val subscription = mockk<Subscription>()
        val subscriptionUri = "urn:ngsi-ld:Subscription:1".toUri()

        every { subscription.id } returns subscriptionUri
        every { subscription.type } returns "Subscription"
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        subscriptionEventService.publishSubscriptionCreateEvent(
            subscription,
            "",
            listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
        )

        verify { kafkaTemplate.send("cim.subscription", subscriptionUri.toString(), any()) }
    }

    @Test
    suspend fun `it should publish an event of type SUBSCRIPTION_UPDATE`() {
        val subscription = mockk<Subscription>()
        val subscriptionUri = "urn:ngsi-ld:Subscription:1".toUri()

        every { subscriptionService.getById(any()) } answers { Mono.just(subscription) }
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        subscriptionEventService.publishSubscriptionUpdateEvent(
            subscriptionUri,
            "",
            listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
        )

        verify { subscriptionService.getById(eq(subscriptionUri)) }
        verify { kafkaTemplate.send("cim.subscription", subscriptionUri.toString(), any()) }
        confirmVerified()
    }

    @Test
    fun `it should publish an event of type NOTIFICATION_CREATE`() {
        val notification = mockk<Notification>(relaxed = true)
        val notificationUri = "urn:ngsi-ld:Notification:1".toUri()

        every { notification.id } returns notificationUri
        every { notification.type } returns "Notification"
        every { notification.notifiedAt } returns Instant.now().atZone(ZoneOffset.UTC)
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        subscriptionEventService.publishNotificationCreateEvent(notification)

        verify { kafkaTemplate.send("cim.notification", notificationUri.toString(), any()) }
    }
}
