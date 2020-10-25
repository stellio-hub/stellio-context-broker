package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver
import org.springframework.messaging.Message
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(classes = [SubscriptionEventService::class])
@ActiveProfiles("test")
class SubscriptionEventServiceTests {

    @Autowired
    private lateinit var subscriptionEventService: SubscriptionEventService

    @MockkBean(relaxed = true)
    private lateinit var resolver: BinderAwareChannelResolver

    private val subscriptionPayload =
        """
        {
          "id":"urn:ngsi-ld:Subscription:1",
          "type":"Subscription",
          "entities": [
            {
              "type": "Beehive"
            },
          ],
          "q": "foodQuantity<150;foodName=='dietary fibres'", 
          "notification": {
            "attributes": ["incoming"],
            "format": "normalized",
            "endpoint": {
              "uri": "http://localhost:8084",
              "accept": "application/json"
            }
          },
          "@context":[
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
          ]
        }
        """.trimIndent()

    private val notificationPayload =
        """
        {
           "id":"urn:ngsi-ld:Notification:1234",
           "type":"Notification",
           "notifiedAt":"2020-03-10T00:00:00Z",
           "subscriptionId":"urn:ngsi-ld:Subscription:1234"
        }
        """.trimIndent()

    @Test
    fun `it should publish an event of type SUBSCRIPTION_CREATE`() {
        val event = SubscriptionCreateEvent("urn:ngsi-ld:Subscription:1".toUri(), subscriptionPayload)
        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        subscriptionEventService.publishSubscriptionEvent(event)

        verify { resolver.resolveDestination("cim.subscription") }

        Assertions.assertTrue(
            message.captured.payload.matchContent(
                loadSampleData("events/subscriptionCreateEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should publish an event of type SUBSCRIPTION_UPDATE`() {
        val event = SubscriptionUpdateEvent(
            "urn:ngsi-ld:Subscription:1".toUri(),
            "{\"q\": \"foodQuantity>=90\"}",
            subscriptionPayload,
            listOf(NGSILD_CORE_CONTEXT)
        )

        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        subscriptionEventService.publishSubscriptionEvent(event)

        verify { resolver.resolveDestination("cim.subscription") }

        Assertions.assertTrue(
            message.captured.payload.matchContent(
                loadSampleData("events/subscriptionUpdateEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should publish an event of type SUBSCRIPTION_DELETE`() {
        val event = SubscriptionDeleteEvent("urn:ngsi-ld:Subscription:1".toUri())

        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        subscriptionEventService.publishSubscriptionEvent(event)

        verify { resolver.resolveDestination("cim.subscription") }

        Assertions.assertTrue(
            message.captured.payload.matchContent(
                loadSampleData("events/subscriptionDeleteEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should publish an event of type NOTIFICATION_CREATE`() {
        val event = NotificationCreateEvent("urn:ngsi-ld:Notification:1".toUri(), notificationPayload)

        val message = slot<Message<String>>()

        every {
            resolver.resolveDestination(any()).send(capture(message))
        } returns true

        subscriptionEventService.publishNotificationEvent(event)

        verify { resolver.resolveDestination("cim.notification") }

        Assertions.assertTrue(
            message.captured.payload.matchContent(
                loadSampleData("events/notificationCreateEvent.jsonld")
            )
        )
    }
}
