package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class JsonUtilsTests {

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private val entityId = "urn:ngsi-ld:Vehicle:A4567".toUri()
    private val subscriptionId = "urn:ngsi-ld:Subscription:1".toUri()
    private val entityPayload =
        """
        {
            "id": "$entityId",
            "type": "Vehicle",
            "brandName": {
                "type": "Property",
                "value": "Mercedes"
            }
        }
        """.trimIndent()

    private val subscriptionPayload =
        """
        {
          "id":"$subscriptionId",
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
          }
        }
        """.trimIndent()

    @Test
    fun `it should parse an event of type ENTITY_CREATE`() {
        val parsedEvent = parseEntityEvent(loadSampleData("events/entityCreateEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityCreateEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_REPLACE`() {
        val parsedEvent = parseEntityEvent(loadSampleData("events/entityReplaceEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityReplaceEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_DELETE`() {
        val parsedEvent = parseEntityEvent(loadSampleData("events/entityDeleteEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_REPLACE`() {
        val parsedEvent = parseEntityEvent(loadSampleData("events/attributeReplaceEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is AttributeReplaceEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_DELETE`() {
        val parsedEvent = parseEntityEvent(loadSampleData("events/attributeDeleteEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is AttributeDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_DELETE_ALL_INSTANCES`() {
        val parsedEvent = parseEntityEvent(loadSampleData("events/attributeDeleteAllInstancesEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is AttributeDeleteAllInstancesEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_UPDATE`() {
        val parsedEvent = parseEntityEvent(loadSampleData("events/entityUpdateEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityUpdateEvent)
    }

    @Test
    fun `it should serialize an event of type ENTITY_CREATE`() {
        val event = mapper.writeValueAsString(
            EntityCreateEvent(
                entityId,
                entityPayload,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        Assertions.assertTrue(event.matchContent(loadSampleData("events/entityCreateEvent.jsonld")))
    }

    @Test
    fun `it should serialize an event of type ENTITY_REPLACE`() {
        val event = mapper.writeValueAsString(
            EntityReplaceEvent(
                entityId,
                entityPayload,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        Assertions.assertTrue(event.matchContent(loadSampleData("events/entityReplaceEvent.jsonld")))
    }

    @Test
    fun `it should serialize an event of type ENTITY_DELETE`() {
        val event = mapper.writeValueAsString(
            EntityDeleteEvent(
                entityId,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        Assertions.assertTrue(event.matchContent(loadSampleData("events/entityDeleteEvent.jsonld")))
    }

    @Test
    fun `it should serialize an event of type ENTITY_UPDATE`() {
        val event = mapper.writeValueAsString(
            EntityUpdateEvent(
                subscriptionId,
                "{\"q\": \"foodQuantity>=90\"}",
                subscriptionPayload,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        Assertions.assertTrue(event.matchContent(loadSampleData("events/entityUpdateEvent.jsonld")))
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_REPLACE`() {
        val event = mapper.writeValueAsString(
            AttributeReplaceEvent(
                entityId,
                "color",
                "urn:ngsi-ld:Dataset:color:1".toUri(),
                "{ \"type\": \"Property\", \"value\": \"red\" }",
                "updatedEntity",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        Assertions.assertTrue(event.matchContent(loadSampleData("events/attributeReplaceEvent.jsonld")))
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_DELETE`() {
        val event = mapper.writeValueAsString(
            AttributeDeleteEvent(
                entityId,
                "color",
                "urn:ngsi-ld:Dataset:color:1".toUri(),
                "updatedEntity",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        Assertions.assertTrue(event.matchContent(loadSampleData("events/attributeDeleteEvent.jsonld")))
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_DELETE_ALL_INSTANCES`() {
        val event = mapper.writeValueAsString(
            AttributeDeleteAllInstancesEvent(
                entityId,
                "color",
                "updatedEntity",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        Assertions.assertTrue(event.matchContent(loadSampleData("events/attributeDeleteAllInstancesEvent.jsonld")))
    }
}
