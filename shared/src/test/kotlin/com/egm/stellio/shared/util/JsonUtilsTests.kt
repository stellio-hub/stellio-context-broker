package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.parseEntitiesEvent
import com.egm.stellio.shared.util.JsonUtils.parseNotificationEvent
import com.egm.stellio.shared.util.JsonUtils.parseSubscriptionEvent
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource

class JsonUtilsTests {

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private val entityPayload =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": {
                "type": "Property",
                "value": "Mercedes"
            },
            "@context": [
                "http://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
            ]
        }
        """.trimIndent()

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

    @Test
    fun `it should serialize an event of type ENTITY_CREATE`() {
        val event = mapper.writeValueAsString(
            EntityCreateEvent("urn:ngsi-ld:Vehicle:A4567".toUri(), entityPayload)
        )

        Assertions.assertTrue(
            event.matchContent(
                loadSampleData("events/entityCreateEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should serialize an event of type ENTITY_DELETE`() {
        val event = mapper.writeValueAsString(
            EntityDeleteEvent("urn:ngsi-ld:Bus:A4567".toUri())
        )

        Assertions.assertTrue(
            event.matchContent(
                loadSampleData("events/entityDeleteEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_REPLACE`() {
        val event = mapper.writeValueAsString(
            AttributeReplaceEvent(
                "urn:ngsi-ld:Bus:A4567".toUri(),
                "color",
                "urn:ngsi-ld:Dataset:color:1".toUri(),
                "{ \"type\": \"Property\", \"value\": \"red\" }",
                "updatedEntity",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )

        Assertions.assertTrue(
            event.matchContent(
                loadSampleData("events/attributeReplaceEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should serialize an event of type SUBSCRIPTION_CREATE`() {
        val event = mapper.writeValueAsString(
            SubscriptionCreateEvent("urn:ngsi-ld:Subscription:1".toUri(), subscriptionPayload)
        )

        Assertions.assertTrue(
            event.matchContent(
                loadSampleData("events/subscriptionCreateEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should serialize an event of type SUBSCRIPTION_UPDATE`() {
        val event = mapper.writeValueAsString(
            SubscriptionUpdateEvent(
                "urn:ngsi-ld:Subscription:1".toUri(),
                "{\"q\": \"foodQuantity>=90\"}",
                subscriptionPayload,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )

        Assertions.assertTrue(
            event.matchContent(
                loadSampleData("events/subscriptionUpdateEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should serialize an event of type SUBSCRIPTION_DELETE`() {
        val event = mapper.writeValueAsString(SubscriptionDeleteEvent("urn:ngsi-ld:Subscription:1".toUri()))

        Assertions.assertTrue(
            event.matchContent(
                loadSampleData("events/subscriptionDeleteEvent.jsonld")
            )
        )
    }

    @Test
    fun `it should serialize an event of type NOTIFICATION_CREATE`() {
        val event = mapper.writeValueAsString(
            NotificationCreateEvent("urn:ngsi-ld:Notification:1".toUri(), notificationPayload)
        )

        Assertions.assertTrue(
            event.matchContent(
                loadSampleData("events/notificationCreateEvent.jsonld")
            )
        )
    }
}
