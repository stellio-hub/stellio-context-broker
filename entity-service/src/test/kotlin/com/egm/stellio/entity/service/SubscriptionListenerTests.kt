package com.egm.stellio.entity.service

import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.loadSampleData
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ SubscriptionListener::class ])
@ActiveProfiles("test")
class SubscriptionListenerTests {

    @Autowired
    private lateinit var subscriptionListener: SubscriptionListener

    @MockkBean
    private lateinit var neo4jService: Neo4jService

    @MockkBean
    private lateinit var ngsiLdParsingUtils: NgsiLdParsingUtils

    @Test
    fun `it should parse and create subscription entity`() {
        val subscription = loadSampleData("subscriptionEvents/subscription_event.jsonld")
        val parsedSubscription = mapOf(
            "id" to "urn:ngsi-ld:Subscription:04",
            "type" to "Subscription",
            "q" to "foodQuantity<150;foodName=='dietary fibres'"
        )

        every { neo4jService.createSubscriptionEntity(any(), any(), any()) } just Runs

        subscriptionListener.processSubscription(subscription)

        verify { neo4jService.createSubscriptionEntity("urn:ngsi-ld:Subscription:04", "Subscription", parsedSubscription.minus("id").minus("type")) }
        confirmVerified(neo4jService)
    }

    @Test
    fun `it should parse and create notification entity`() {
        val notification = loadSampleData("subscriptionEvents/notification_event.jsonld")
        val parsedNotification = mapOf(
            "id" to "urn:ngsi-ld:Notification:1234",
            "type" to "Notification",
            "notifiedAt" to "2020-03-10T00:00:00Z",
            "subscriptionId" to "urn:ngsi-ld:Subscription:1234"
        )

        every { neo4jService.createNotificationEntity(any(), any(), any(), any()) } just Runs

        subscriptionListener.processNotification(notification)

        verify { neo4jService.createNotificationEntity("urn:ngsi-ld:Notification:1234", "Notification", "urn:ngsi-ld:Subscription:1234",
            parsedNotification.minus("id").minus("type").minus("subscriptionId")) }
        confirmVerified(neo4jService)
    }
}
