package com.egm.stellio.subscription.listener

import arrow.core.right
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.NotificationTrigger
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.NotificationService
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.mockkClass
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityEventListenerService::class])
@ActiveProfiles("test")
class EntityEventListenerServiceTests {

    @Autowired
    private lateinit var entityEventListenerService: EntityEventListenerService

    @MockkBean
    private lateinit var notificationService: NotificationService

    private val entityId = "urn:ngsi-ld:BeeHive:01".toUri()

    @Test
    fun `it should parse and transmit an entity create event`() = runTest {
        val entityCreateEvent = loadSampleData("events/entity/entityCreateEvent.json")

        mockNotificationService()

        entityEventListenerService.dispatchEntityEvent(entityCreateEvent)

        coVerify(timeout = 1000L) {
            notificationService.notifyMatchingSubscribers(
                DEFAULT_TENANT_NAME,
                null,
                null,
                match {
                    it.id == entityId
                },
                NotificationTrigger.ENTITY_CREATED
            )
        }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should parse and transmit an entity delete event`() = runTest {
        val entityDeleteEvent = loadSampleData("events/entity/entityDeleteEvent.json")

        mockNotificationService()

        entityEventListenerService.dispatchEntityEvent(entityDeleteEvent)

        coVerify(timeout = 1000L) {
            notificationService.notifyMatchingSubscribers(
                DEFAULT_TENANT_NAME,
                null,
                any(),
                any(),
                NotificationTrigger.ENTITY_DELETED
            )
        }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should parse and transmit an attribute create event`() = runTest {
        val attributeCreateEvent = loadSampleData("events/entity/attributeAppendTextPropEvent.json")

        mockNotificationService()

        entityEventListenerService.dispatchEntityEvent(attributeCreateEvent)

        coVerify(timeout = 1000L) {
            notificationService.notifyMatchingSubscribers(
                DEFAULT_TENANT_NAME,
                Pair(NAME_IRI, null),
                any(),
                match {
                    it.id == entityId
                },
                NotificationTrigger.ATTRIBUTE_CREATED
            )
        }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should parse and transmit an attribute update event`() = runTest {
        val attributeUpdateEvent = loadSampleData("events/entity/attributeUpdateTextPropEvent.json")

        mockNotificationService()

        entityEventListenerService.dispatchEntityEvent(attributeUpdateEvent)

        coVerify(timeout = 1000L) {
            notificationService.notifyMatchingSubscribers(
                DEFAULT_TENANT_NAME,
                Pair(NAME_IRI, null),
                any(),
                match {
                    it.id == entityId
                },
                NotificationTrigger.ATTRIBUTE_UPDATED
            )
        }
        confirmVerified(notificationService)
    }

    @Test
    fun `it should parse and transmit an attribute delete event`() = runTest {
        val attributeDeleteEvent = loadSampleData("events/entity/attributeDeleteEvent.json")

        mockNotificationService()

        entityEventListenerService.dispatchEntityEvent(attributeDeleteEvent)

        coVerify(timeout = 1000L) {
            notificationService.notifyMatchingSubscribers(
                DEFAULT_TENANT_NAME,
                Pair(TEMPERATURE_IRI, null),
                any(),
                match {
                    it.id == entityId
                },
                NotificationTrigger.ATTRIBUTE_DELETED
            )
        }
        confirmVerified(notificationService)
    }

    private fun mockNotificationService() {
        val mockedSubscription = mockkClass(Subscription::class)
        val mockedNotification = mockkClass(Notification::class)

        coEvery {
            notificationService.notifyMatchingSubscribers(DEFAULT_TENANT_NAME, any(), any(), any(), any())
        } returns listOf(
            Triple(mockedSubscription, mockedNotification, true),
            Triple(mockedSubscription, mockedNotification, false)
        ).right()
    }
}
