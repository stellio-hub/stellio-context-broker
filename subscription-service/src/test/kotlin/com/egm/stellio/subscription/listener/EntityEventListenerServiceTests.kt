package com.egm.stellio.subscription.listener

import arrow.core.right
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.NotificationTrigger
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.NotificationService
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.mockkClass
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityEventListenerService::class])
@ActiveProfiles("test")
class EntityEventListenerServiceTests {

    @Autowired
    private lateinit var entityEventListenerService: EntityEventListenerService

    @MockkBean
    private lateinit var notificationService: NotificationService

    @Test
    fun `it should parse and transmit an attribute create events for an entity replace event`() = runTest {
        val replaceEvent = loadSampleData("events/entity/entityReplaceEvent.json")

        val mockedSubscription = mockkClass(Subscription::class)
        val mockedNotification = mockkClass(Notification::class)

        coEvery {
            notificationService.notifyMatchingSubscribers(any(), any(), any(), any())
        } returns listOf(
            Triple(mockedSubscription, mockedNotification, true),
            Triple(mockedSubscription, mockedNotification, false)
        ).right()

        entityEventListenerService.dispatchEntityEvent(replaceEvent)

        coVerify(exactly = 4, timeout = 1000L) {
            notificationService.notifyMatchingSubscribers(any(), any(), any(), any())
        }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should parse and transmit an attribute replace event`() = runTest {
        val replaceEvent = loadSampleData("events/entity/attributeReplaceTextPropEvent.json")

        val mockedSubscription = mockkClass(Subscription::class)
        val mockedNotification = mockkClass(Notification::class)

        coEvery {
            notificationService.notifyMatchingSubscribers(any(), any(), any(), any())
        } returns listOf(
            Triple(mockedSubscription, mockedNotification, true),
            Triple(mockedSubscription, mockedNotification, false)
        ).right()

        entityEventListenerService.dispatchEntityEvent(replaceEvent)

        coVerify(timeout = 1000L) { notificationService.notifyMatchingSubscribers(any(), any(), any(), any()) }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should parse and transmit an attribute update event`() = runTest {
        val updateEvent = loadSampleData("events/entity/attributeUpdateTextPropEvent.json")

        val mockedSubscription = mockkClass(Subscription::class)
        val mockedNotification = mockkClass(Notification::class)

        coEvery {
            notificationService.notifyMatchingSubscribers(any(), any(), any(), any())
        } returns listOf(
            Triple(mockedSubscription, mockedNotification, true),
            Triple(mockedSubscription, mockedNotification, false)
        ).right()

        entityEventListenerService.dispatchEntityEvent(updateEvent)

        coVerify(timeout = 1000L) {
            notificationService.notifyMatchingSubscribers(
                any(),
                any(),
                setOf(NGSILD_NAME_PROPERTY),
                NotificationTrigger.ATTRIBUTE_UPDATED
            )
        }
        confirmVerified(notificationService)
    }
}
