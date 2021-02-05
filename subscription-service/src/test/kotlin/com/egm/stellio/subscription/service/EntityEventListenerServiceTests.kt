package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.Notification
import com.egm.stellio.subscription.model.Subscription
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono

@SpringBootTest(classes = [EntityEventListenerService::class])
@ActiveProfiles("test")
class EntityEventListenerServiceTests {

    @Autowired
    private lateinit var entityEventListenerService: EntityEventListenerService

    @MockkBean
    private lateinit var notificationService: NotificationService

    @Test
    fun `it should parse and transmit an attribute replace event`() {
        val replaceEvent = ClassPathResource("/ngsild/events/listened/AttributeReplaceEvent.json")

        val mockedSubscription = mockkClass(Subscription::class)
        val mockedNotification = mockkClass(Notification::class)

        every { notificationService.notifyMatchingSubscribers(any(), any(), any()) } answers {
            Mono.just(
                listOf(
                    Triple(mockedSubscription, mockedNotification, true),
                    Triple(mockedSubscription, mockedNotification, false)
                )
            )
        }

        entityEventListenerService.processMessage(replaceEvent.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { notificationService.notifyMatchingSubscribers(any(), any(), any()) }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should parse and transmit an attribute update event`() {
        val updateEvent = ClassPathResource("/ngsild/events/listened/AttributeUpdateEvent.json")

        val mockedSubscription = mockkClass(Subscription::class)
        val mockedNotification = mockkClass(Notification::class)

        every { notificationService.notifyMatchingSubscribers(any(), any(), any()) } answers {
            Mono.just(
                listOf(
                    Triple(mockedSubscription, mockedNotification, true),
                    Triple(mockedSubscription, mockedNotification, false)
                )
            )
        }

        entityEventListenerService.processMessage(updateEvent.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { notificationService.notifyMatchingSubscribers(any(), any(), setOf("name")) }
        confirmVerified(notificationService)
    }
}
