package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.utils.gimmeRawSubscription
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import java.util.concurrent.CompletableFuture

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [SubscriptionEventService::class])
@ActiveProfiles("test")
class SubscriptionEventServiceTests {

    @Autowired
    private lateinit var subscriptionEventService: SubscriptionEventService

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean
    private lateinit var subscriptionService: SubscriptionService

    @Test
    fun `it should publish an event of type SUBSCRIPTION_CREATE`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getById(any()) } returns subscription
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        subscriptionEventService.publishSubscriptionCreateEvent(
            null,
            subscription.id,
            listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
        ).join()

        coVerify { subscriptionService.getById(eq(subscription.id)) }
        verify { kafkaTemplate.send("cim.subscription", subscription.id.toString(), any()) }
    }

    @Test
    fun `it should publish an event of type SUBSCRIPTION_UPDATE`() = runTest {
        val subscription = gimmeRawSubscription()

        coEvery { subscriptionService.getById(any()) } returns subscription
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        subscriptionEventService.publishSubscriptionUpdateEvent(
            null,
            subscription.id,
            "",
            listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
        ).join()

        coVerify { subscriptionService.getById(eq(subscription.id)) }
        verify { kafkaTemplate.send("cim.subscription", subscription.id.toString(), any()) }
    }

    @Test
    fun `it should publish an event of type SUBSCRIPTION_DELETE`() = runTest {
        val subscriptionUri = "urn:ngsi-ld:Subscription:1".toUri()

        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        subscriptionEventService.publishSubscriptionDeleteEvent(
            null,
            subscriptionUri,
            listOf(NGSILD_EGM_CONTEXT, NGSILD_CORE_CONTEXT)
        ).join()

        verify { kafkaTemplate.send("cim.subscription", subscriptionUri.toString(), any()) }
    }

    @Test
    fun `it should publish an event of type NOTIFICATION_CREATE`() = runTest {
        val notification = Notification(
            subscriptionId = "urn:ngsi-ld:Subscription:1".toUri(),
            data = emptyList()
        )
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()

        subscriptionEventService.publishNotificationCreateEvent(null, notification).join()

        verify { kafkaTemplate.send("cim.notification", notification.id.toString(), any()) }
    }
}
