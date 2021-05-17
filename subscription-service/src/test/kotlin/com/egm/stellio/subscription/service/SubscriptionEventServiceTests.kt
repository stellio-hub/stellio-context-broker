package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.concurrent.SettableListenableFuture

@SpringBootTest(classes = [SubscriptionEventService::class])
@ActiveProfiles("test")
class SubscriptionEventServiceTests {

    @Autowired
    private lateinit var subscriptionEventService: SubscriptionEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Test
    fun `it should publish an event of type SUBSCRIPTION_CREATE`() {
        val subscriptionUri = "urn:ngsi-ld:Subscription:1".toUri()
        val event = EntityCreateEvent(
            subscriptionUri,
            "operationPayload",
            listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
        )
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        subscriptionEventService.publishSubscriptionEvent(event)

        verify { kafkaTemplate.send("cim.subscription", subscriptionUri.toString(), any()) }
    }

    @Test
    fun `it should publish an event of type NOTIFICATION_CREATE`() {
        val notificationUri = "urn:ngsi-ld:Notification:1".toUri()
        val event = EntityCreateEvent(
            notificationUri,
            "operationPayload",
            listOf(JsonLdUtils.NGSILD_EGM_CONTEXT, JsonLdUtils.NGSILD_CORE_CONTEXT)
        )
        every { kafkaTemplate.send(any(), any(), any()) } returns SettableListenableFuture()

        subscriptionEventService.publishNotificationEvent(event)

        verify { kafkaTemplate.send("cim.notification", notificationUri.toString(), any()) }
    }
}
