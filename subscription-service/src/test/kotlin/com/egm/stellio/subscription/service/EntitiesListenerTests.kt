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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntitiesListener::class])
@ActiveProfiles("test")
class EntitiesListenerTests {

    @Autowired
    private lateinit var entitiesListener: EntitiesListener

    @MockkBean
    private lateinit var notificationService: NotificationService

    @Test
    fun `it should parse and transmit observations`() {
        val observation = ClassPathResource("/ngsild/apic/BeekeeperUpdateEvent.json")

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

        entitiesListener.processMessage(observation.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { notificationService.notifyMatchingSubscribers(any(), any(), any()) }
        confirmVerified(notificationService)
    }
}
