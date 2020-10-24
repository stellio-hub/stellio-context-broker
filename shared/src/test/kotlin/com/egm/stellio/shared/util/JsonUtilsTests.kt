package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.parseEntitiesEvent
import com.egm.stellio.shared.util.JsonUtils.parseNotificationEvent
import com.egm.stellio.shared.util.JsonUtils.parseSubscriptionEvent
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource

class JsonUtilsTests {

    @Test
    fun `it should parse an event of type ENTITY_CREATE`() {
        val parsedEvent = parseEntitiesEvent(
            ClassPathResource("/ngsild/events/entityCreateEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is EntityCreateEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_DELETE`() {
        val parsedEvent = parseEntitiesEvent(
            ClassPathResource("/ngsild/events/entityDeleteEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is EntityDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_REPLACE`() {
        val parsedEvent = parseEntitiesEvent(
            ClassPathResource("/ngsild/events/attributeReplaceEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is AttributeReplaceEvent)
    }

    @Test
    fun `it should parse an event of type SUBSCRIPTION_CREATE`() {
        val parsedEvent = parseSubscriptionEvent(
            ClassPathResource("/ngsild/events/subscriptionCreateEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is SubscriptionCreateEvent)
    }

    @Test
    fun `it should parse an event of type SUBSCRIPTION_UPDATE`() {
        val parsedEvent = parseSubscriptionEvent(
            ClassPathResource("/ngsild/events/subscriptionUpdateEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is SubscriptionUpdateEvent)
    }

    @Test
    fun `it should parse an event of type SUBSCRIPTION_DELETE`() {
        val parsedEvent = parseSubscriptionEvent(
            ClassPathResource("/ngsild/events/subscriptionDeleteEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is SubscriptionDeleteEvent)
    }

    @Test
    fun `it should parse an event of type NOTIFICATION_CREATE`() {
        val parsedEvent = parseNotificationEvent(
            ClassPathResource("/ngsild/events/notificationCreateEvent.jsonld")
                .inputStream.readBytes().toString(Charsets.UTF_8)
        )
        Assertions.assertTrue(parsedEvent is NotificationCreateEvent)
    }
}
