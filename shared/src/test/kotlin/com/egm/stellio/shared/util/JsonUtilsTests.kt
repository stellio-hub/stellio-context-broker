package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
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
    private val entityType = "Vehicle"
    private val subscriptionId = "urn:ngsi-ld:Subscription:1".toUri()
    private val subscriptionType = "Subscription"
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
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entityCreateEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityCreateEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_REPLACE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entityReplaceEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityReplaceEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_DELETE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entityDeleteEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_UPDATE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/attributeUpdateEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is AttributeUpdateEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_REPLACE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/attributeReplaceEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is AttributeReplaceEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_DELETE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/attributeDeleteEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is AttributeDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_DELETE_ALL_INSTANCES`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/attributeDeleteAllInstancesEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is AttributeDeleteAllInstancesEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_UPDATE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entityUpdateEvent.jsonld"))
        Assertions.assertTrue(parsedEvent is EntityUpdateEvent)
    }

    @Test
    fun `it should serialize an event of type ENTITY_CREATE`() {
        val event = mapper.writeValueAsString(
            EntityCreateEvent(
                entityId,
                entityType,
                entityPayload,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/entityCreateEvent.jsonld"))
    }

    @Test
    fun `it should serialize an event of type ENTITY_REPLACE`() {
        val event = mapper.writeValueAsString(
            EntityReplaceEvent(
                entityId,
                entityType,
                entityPayload,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/entityReplaceEvent.jsonld"))
    }

    @Test
    fun `it should serialize an event of type ENTITY_DELETE`() {
        val event = mapper.writeValueAsString(
            EntityDeleteEvent(
                entityId,
                entityType,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/entityDeleteEvent.jsonld"))
    }

    @Test
    fun `it should serialize an event of type ENTITY_UPDATE`() {
        val event = mapper.writeValueAsString(
            EntityUpdateEvent(
                subscriptionId,
                subscriptionType,
                "{\"q\": \"foodQuantity>=90\"}",
                subscriptionPayload,
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/entityUpdateEvent.jsonld"))
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_UPDATE`() {
        val event = mapper.writeValueAsString(
            AttributeUpdateEvent(
                entityId,
                entityType,
                "color",
                "urn:ngsi-ld:Dataset:color:1".toUri(),
                "{ \"value\":76, \"unitCode\": \"CEL\", \"observedAt\": \"2019-10-26T22:35:52.98601Z\" }",
                "",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/attributeUpdateEvent.jsonld"))
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_REPLACE`() {
        val event = mapper.writeValueAsString(
            AttributeReplaceEvent(
                entityId,
                entityType,
                "color",
                "urn:ngsi-ld:Dataset:color:1".toUri(),
                "{ \"type\": \"Property\", \"value\": \"red\" }",
                "updatedEntity",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/attributeReplaceEvent.jsonld"))
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_DELETE`() {
        val event = mapper.writeValueAsString(
            AttributeDeleteEvent(
                entityId,
                entityType,
                "color",
                "urn:ngsi-ld:Dataset:color:1".toUri(),
                "updatedEntity",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/attributeDeleteEvent.jsonld"))
    }

    @Test
    fun `it should serialize an event of type ATTRIBUTE_DELETE_ALL_INSTANCES`() {
        val event = mapper.writeValueAsString(
            AttributeDeleteAllInstancesEvent(
                entityId,
                entityType,
                "color",
                "updatedEntity",
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(event, loadSampleData("events/attributeDeleteAllInstancesEvent.jsonld"))
    }
}
