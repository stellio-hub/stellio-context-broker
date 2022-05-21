package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class JsonUtilsTests {

    private val entityId = "urn:ngsi-ld:BeeHive:01".toUri()
    private val entityPayload =
        """
        {
            "id":"urn:ngsi-ld:BeeHive:01",
            "type":"BeeHive",
            "humidity":{
                "type":"Property",
                "observedBy":{
                    "type":"Relationship",
                    "createdAt":"2022-02-12T08:36:59.455870Z",
                    "object":"urn:ngsi-ld:Sensor:02"
                },
                "createdAt":"2022-02-12T08:36:59.448205Z",
                "value":60,
                "observedAt":"2019-10-26T21:32:52.986010Z",
                "unitCode":"P1"
            },
            "temperature":{
                "type":"Property",
                "observedBy":{
                    "type":"Relationship",
                    "createdAt":"2022-02-12T08:36:59.473904Z",
                    "object":"urn:ngsi-ld:Sensor:01"
                },
                "createdAt":"2022-02-12T08:36:59.465937Z",
                "value":22.2,
                "observedAt":"2019-10-26T21:32:52.986010Z",
                "unitCode":"CEL"
            },
            "belongs":{
                "type":"Relationship",
                "createdAt":"2022-02-12T08:36:59.389815Z",
                "object":"urn:ngsi-ld:Apiary:01"
            },
            "managedBy":{
                "type":"Relationship",
                "createdAt":"2022-02-12T08:36:59.417938Z",
                "object":"urn:ngsi-ld:Beekeeper:01"
            },
            "createdAt":"2022-02-12T08:36:59.179446Z",
            "location":{
                "type":"GeoProperty",
                "value":{
                    "type":"Point",
                    "coordinates":[24.30623,60.07966]
                }
            },
            "modifiedAt":"2022-02-12T08:36:59.218595Z"
        }
        """.trimIndent().replace(" ", "").replace("\n", "")

    @Test
    fun `it should parse an event of type ENTITY_CREATE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entity/entityCreateEvent.json"))
        Assertions.assertTrue(parsedEvent is EntityCreateEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_REPLACE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entity/entityReplaceEvent.json"))
        Assertions.assertTrue(parsedEvent is EntityReplaceEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_DELETE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entity/entityDeleteEvent.json"))
        Assertions.assertTrue(parsedEvent is EntityDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_UPDATE`() {
        val parsedEvent = deserializeAs<EntityEvent>(
            loadSampleData("events/entity/attributeUpdateTextPropEvent.json")
        )
        Assertions.assertTrue(parsedEvent is AttributeUpdateEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_REPLACE`() {
        val parsedEvent = deserializeAs<EntityEvent>(
            loadSampleData("events/entity/attributeReplaceTextPropEvent.json")
        )
        Assertions.assertTrue(parsedEvent is AttributeReplaceEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_DELETE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entity/attributeDeleteEvent.json"))
        Assertions.assertTrue(parsedEvent is AttributeDeleteEvent)
    }

    @Test
    fun `it should parse an event of type ATTRIBUTE_DELETE_ALL_INSTANCES`() {
        val parsedEvent = deserializeAs<EntityEvent>(
            loadSampleData("events/entity/attributeDeleteAllInstancesEvent.json")
        )
        Assertions.assertTrue(parsedEvent is AttributeDeleteAllInstancesEvent)
    }

    @Test
    fun `it should parse an event of type ENTITY_UPDATE`() {
        val parsedEvent = deserializeAs<EntityEvent>(loadSampleData("events/entity/entityUpdateEvent.json"))
        Assertions.assertTrue(parsedEvent is EntityUpdateEvent)
    }

    @Test
    fun `it should serialize an event of type ENTITY_CREATE`() {
        val event = mapper.writeValueAsString(
            EntityCreateEvent(
                "0123456789-1234-5678-987654321",
                entityId,
                listOf(BEEHIVE_COMPACT_TYPE),
                entityPayload,
                listOf(APIC_COMPOUND_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(loadSampleData("events/entity/entityCreateEvent.json"), event)
    }

    @Test
    fun `it should serialize an event of type ENTITY_DELETE`() {
        val event = mapper.writeValueAsString(
            EntityDeleteEvent(
                null,
                entityId,
                listOf(BEEHIVE_COMPACT_TYPE),
                listOf(APIC_COMPOUND_CONTEXT)
            )
        )
        assertJsonPayloadsAreEqual(loadSampleData("events/entity/entityDeleteEvent.json"), event)
    }

    @Test
    fun `it should throw an InvalidRequest exception if the JSON-LD fragment is not a valid JSON document`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",,
                "type": "Device"
            }
            """.trimIndent()

        val exception = assertThrows<InvalidRequestException> {
            rawEntity.deserializeAsMap()
        }
        Assertions.assertEquals(
            """
                Unexpected character (',' (code 44)): was expecting double-quote to start field name
                 at [Source: (String)"{
                    "id": "urn:ngsi-ld:Device:01234",,
                    "type": "Device"
                }"; line: 2, column: 39]
            """.trimIndent(),
            exception.message
        )
    }
}
