package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.ZonedDateTime

class NgsiLdEntityTests {

    @Test
    fun `it should parse a minimal entity`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS)

        assertEquals("urn:ngsi-ld:Device:01234", ngsiLdEntity.id)
        assertEquals("https://uri.fiware.org/ns/data-models#Device", ngsiLdEntity.type)
    }

    @Test
    fun `it should not parse an entity without an id`() {
        val rawEntity =
            """
            {
                "type": "Device"
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()
        }
        assertEquals(
            "The provided NGSI-LD entity does not contain an id property",
            exception.message
        )
    }

    @Test
    fun `it should not parse an entity without a type`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "deviceState": {
                    "type": "Property",
                    "value": 23
                }
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()
        }
        assertEquals(
            "The provided NGSI-LD entity does not contain a type property",
            exception.message
        )
    }

    @Test
    fun `it should parse an entity with a minimal property`() {
        val rawEntity =
            """
            {
              "id": "urn:ngsi-ld:Device:01234",
              "type": "Device",
              "deviceState": {
                "type": "Property",
                "value": "Open"
              }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        assertEquals(1, ngsiLdEntity.properties.size)
        val ngsiLdProperty = ngsiLdEntity.properties[0]
        assertEquals("https://uri.fiware.org/ns/data-models#deviceState", ngsiLdProperty.name)
        assertEquals(1, ngsiLdProperty.instances.size)
        val ngsiLdPropertyInstance = ngsiLdProperty.instances[0]
        assertEquals("Open", ngsiLdPropertyInstance.value)
    }

    @Test
    fun `it should parse an entity with a property having all core fields`() {
        val rawEntity =
            """
            {
              "id": "urn:ngsi-ld:Device:01234",
              "type": "Device",
              "deviceState": {
                "type": "Property",
                "value": "Open",
                "unitCode": "MTR",
                "datasetId": "urn:ngsi-ld:Dataset:01234",
                "observedAt": "2020-07-19T00:00:00Z"
              }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val ngsiLdPropertyInstance = ngsiLdEntity.properties[0].instances[0]
        assertEquals("Open", ngsiLdPropertyInstance.value)
        assertEquals("MTR", ngsiLdPropertyInstance.unitCode)
        assertEquals("urn:ngsi-ld:Dataset:01234".toUri(), ngsiLdPropertyInstance.datasetId)
        assertEquals(ZonedDateTime.parse("2020-07-19T00:00:00Z"), ngsiLdPropertyInstance.observedAt)
    }

    @Test
    fun `it should parse an entity with a multi-attribute property`() {
        val rawEntity =
            """
            {
              "id": "urn:ngsi-ld:Device:01234",
              "type": "Device",
              "deviceState": [{
                "type": "Property",
                "value": "Open",
                "datasetId": "urn:ngsi-ld:Dataset:01234"
              },
              {
                "type": "Property",
                "value": "Closed",
                "datasetId": "urn:ngsi-ld:Dataset:45678"
              }]
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        assertEquals(1, ngsiLdEntity.properties.size)
        val ngsiLdProperty = ngsiLdEntity.properties[0]
        assertEquals(2, ngsiLdProperty.instances.size)
        assertArrayEquals(
            arrayOf("urn:ngsi-ld:Dataset:01234".toUri(), "urn:ngsi-ld:Dataset:45678".toUri()),
            ngsiLdProperty.instances.map { it.datasetId }.toTypedArray()
        )
    }

    @Test
    fun `it should not parse a property with different type instances`() {
        val rawProperty =
            """
            {
                "deviceState": [{
                    "type": "Property",
                    "value": 600
                },
                {
                    "type":"Relationship",
                    "object":"urn:ngsi-ld:Beekeeper:654321",
                    "datasetId": "urn:ngsi-ld:Dataset:fishNumber:1"
                }]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseToNgsiLdAttributes(
                expandJsonLdFragment(rawProperty, DEFAULT_CONTEXTS)
            )
        }
        assertEquals(
            "Attribute https://uri.fiware.org/ns/data-models#deviceState instances must have the same type",
            exception.message
        )
    }

    @Test
    fun `it should not parse a property with a duplicated datasetId`() {
        val rawProperty =
            """
            {
                "deviceState": [
                    {
                        "type": "Property",
                        "value": 35
                    },
                    {
                        "type": "Property",
                        "value": 12,
                        "datasetId": "urn:ngsi-ld:Dataset:fishName:1"
                    },
                    {
                        "type": "Property",
                        "value": 20,
                        "datasetId": "urn:ngsi-ld:Dataset:fishName:2"
                    },
                    {
                        "type": "Property",
                        "value": 14,
                        "datasetId": "urn:ngsi-ld:Dataset:fishName:1"
                    }
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseToNgsiLdAttributes(
                expandJsonLdFragment(rawProperty, DEFAULT_CONTEXTS)
            )
        }
        assertEquals(
            "Attribute https://uri.fiware.org/ns/data-models#deviceState" +
                " can't have more than one instance with the same datasetId",
            exception.message
        )
    }

    @Test
    fun `it should not parse a property with more than one default instance`() {
        val rawProperty =
            """
            {
                "deviceState": [
                    {
                        "type": "Property",
                        "value": 35
                    },
                    {
                        "type": "Property",
                        "value": 12
                    },
                    {
                        "type": "Property",
                        "value": 14,
                        "datasetId": "urn:ngsi-ld:Dataset:fishName:1"
                    }
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseToNgsiLdAttributes(
                expandJsonLdFragment(rawProperty, DEFAULT_CONTEXTS)
            )
        }
        assertEquals(
            "Attribute https://uri.fiware.org/ns/data-models#deviceState can't have more than one default instance",
            exception.message
        )
    }

    @Test
    fun `it should parse a property with a datasetId`() {
        val rawProperty =
            """
            {
                "deviceState": [
                    {
                        "type": "Property",
                        "value": 35,
                        "datasetId": "urn:ngsi-ld:Dataset:fishName:1"
                    }
                ]
            }
            """.trimIndent()

        val ngsiLdAttributes = parseToNgsiLdAttributes(expandJsonLdFragment(rawProperty, DEFAULT_CONTEXTS))

        assertEquals(1, ngsiLdAttributes.size)
        val ngsiLdAttribute = ngsiLdAttributes[0]
        assertTrue(ngsiLdAttribute is NgsiLdProperty)
        val ngsiLdPropertyInstance = (ngsiLdAttribute as NgsiLdProperty).instances[0]
        assertEquals("urn:ngsi-ld:Dataset:fishName:1".toUri(), ngsiLdPropertyInstance.datasetId)
        assertEquals(35, ngsiLdPropertyInstance.value)
    }

    @Test
    fun `it should parse an entity with a minimal relationship`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "refDeviceModel": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:DeviceModel:09876"
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        assertEquals(1, ngsiLdEntity.relationships.size)
        val ngsiLdRelationship = ngsiLdEntity.relationships[0]
        assertEquals("https://uri.fiware.org/ns/data-models#refDeviceModel", ngsiLdRelationship.name)
        assertEquals(1, ngsiLdRelationship.instances.size)
        val ngsiLdRelationshipInstance = ngsiLdRelationship.instances[0]
        assertEquals("urn:ngsi-ld:DeviceModel:09876".toUri(), ngsiLdRelationshipInstance.objectId)
    }

    @Test
    fun `it should parse an entity with a relationship having all core fields`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "refDeviceModel": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:DeviceModel:09876",
                    "datasetId": "urn:ngsi-ld:Dataset:01234",
                    "observedAt": "2020-07-19T00:00:00Z"
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val ngsiLdRelationshipInstance = ngsiLdEntity.relationships[0].instances[0]
        assertEquals("urn:ngsi-ld:DeviceModel:09876".toUri(), ngsiLdRelationshipInstance.objectId)
        assertEquals("urn:ngsi-ld:Dataset:01234".toUri(), ngsiLdRelationshipInstance.datasetId)
        assertEquals(ZonedDateTime.parse("2020-07-19T00:00:00Z"), ngsiLdRelationshipInstance.observedAt)
    }

    @Test
    fun `it should parse an entity with a multi-attribute relationship`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "refDeviceModel": [
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:DeviceModel:09876",
                        "datasetId": "urn:ngsi-ld:Dataset:01234"
                    },
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:DeviceModel:12345",
                        "datasetId": "urn:ngsi-ld:Dataset:56789"
                    }
                ]
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        assertEquals(1, ngsiLdEntity.relationships.size)
        val ngsiLdRelationship = ngsiLdEntity.relationships[0]
        assertEquals(2, ngsiLdRelationship.instances.size)
    }

    @Test
    fun `it should not parse a relationship with more than one default instance`() {
        val rawRelationship =
            """
            {
                "refDeviceModel": [
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:BreedingService:0214"
                    },
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:BreedingService:0215"
                    }
                ]
            }

            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseToNgsiLdAttributes(
                expandJsonLdFragment(rawRelationship, DEFAULT_CONTEXTS)
            )
        }
        assertEquals(
            "Attribute https://uri.fiware.org/ns/data-models#refDeviceModel can't have more than one default instance",
            exception.message
        )
    }

    @Test
    fun `it should not parse a relationship with a duplicated datasetId`() {
        val rawRelationship =
            """
            {
                "refDeviceModel": [
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:BreedingService:0214",
                        "datasetId": "urn:ngsi-ld:Dataset:removedFrom:0214"
                    },
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:BreedingService:0215",
                        "datasetId": "urn:ngsi-ld:Dataset:removedFrom:0214"
                    }
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseToNgsiLdAttributes(
                expandJsonLdFragment(rawRelationship, DEFAULT_CONTEXTS)
            )
        }
        assertEquals(
            "Attribute https://uri.fiware.org/ns/data-models#refDeviceModel " +
                "can't have more than one instance with the same datasetId",
            exception.message
        )
    }

    @Test
    fun `it should not parse a relationship with different type instances`() {
        val rawRelationship =
            """
            {
                "refDeviceModel": [
                    {
                        "type":"Relationship",
                        "object":"urn:ngsi-ld:DeviceModel:0214",
                        "datasetId": "urn:ngsi-ld:Dataset:0214"
                    },
                    {
                        "type": "Property",
                        "value": 35
                    },
                    {
                        "type":"Relationship",
                        "object":"urn:ngsi-ld:DeviceModel:0215",
                        "datasetId": "urn:ngsi-ld:Dataset:0215"
                    }
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            parseToNgsiLdAttributes(
                expandJsonLdFragment(rawRelationship, DEFAULT_CONTEXTS)
            )
        }
        assertEquals(
            "Attribute https://uri.fiware.org/ns/data-models#refDeviceModel instances must have the same type",
            exception.message
        )
    }

    @Test
    fun `it should parse an entity with a Polygon location`() {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Polygon",
                        "coordinates": [
                            [100.0, 0.0],
                            [101.0, 0.0], 
                            [101.0, 1.0],
                            [100.0, 1.0],
                            [100.0, 0.0]
                        ]
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val location = ngsiLdEntity.getLocation()
        assertNotNull(location)
        assertEquals("https://uri.etsi.org/ngsi-ld/location", location?.name)
        assertEquals(1, location!!.instances.size)
        val locationInstance = location.instances[0]
        assertEquals("Polygon", locationInstance.geoPropertyType)
        val coordinates = listOf(
            listOf(100.0, 0.0),
            listOf(101.0, 0.0),
            listOf(101.0, 1.0),
            listOf(100.0, 1.0),
            listOf(100.0, 0.0)
        )
        assertEquals(coordinates, locationInstance.coordinates)
    }

    @Test
    fun `it should parse an entity with a Point location`() {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [
                            24.30623,
                            60.07966
                        ]
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val location = ngsiLdEntity.getLocation()
        assertNotNull(location)
        assertEquals("https://uri.etsi.org/ngsi-ld/location", location?.name)
        assertEquals(1, location!!.instances.size)
        val locationInstance = location.instances[0]
        assertEquals("Point", locationInstance.geoPropertyType)
        assertEquals(listOf(24.30623, 60.07966), locationInstance.coordinates)

        val (long, lat) = ngsiLdEntity.extractCoordinatesFromLocationPoint()
        assertEquals(24.30623, long)
        assertEquals(60.07966, lat)
    }

    @Test
    fun `it should find relationships of entity`() {
        val expandedEntity = expandJsonLdEntity(
            """
            { 
                "id": "urn:ngsi-ld:Vehicle:A12388", 
                "type": "Vehicle", 
                "connectsTo": { 
                    "type": "Relationship",
                    "object": "relation1"
                }
            }
            """.trimIndent(),
            DEFAULT_CONTEXTS
        ).toNgsiLdEntity()

        assertEquals(arrayListOf("relation1").toListOfUri(), expandedEntity.getLinkedEntitiesIds())
    }

    @Test
    fun `it should find relationships of properties`() {
        val expandedEntity = expandJsonLdEntity(
            """
            { 
                 "id": "urn:ngsi-ld:Vehicle:A12388",
                 "type": "Vehicle",
                 "connectsTo": {
                    "type": "Relationship",
                    "object": "relation1"
                 },
                 "speed": {
                    "type": "Property", 
                    "value": 35, 
                    "flashedFrom": { 
                        "type": "Relationship", 
                        "object": "Radar" 
                    }
                }
            }
            """.trimIndent(),
            DEFAULT_CONTEXTS
        ).toNgsiLdEntity()

        assertEquals(listOf("Radar", "relation1").toListOfUri(), expandedEntity.getLinkedEntitiesIds())
    }

    @Test
    fun `it should find relationships of relationships`() {
        val expandedEntity = expandJsonLdEntity(
            """
            { 
                "id" : "urn:ngsi-ld:Vehicle:A12388",
                "type": "Vehicle",
                "connectsTo": {
                    "type": "Relationship",
                    "object": "relation1",
                    "createdBy ": {
                        "type": "Relationship",
                        "object": "relation2"
                    }
                }
            }
            """.trimIndent(),
            DEFAULT_CONTEXTS
        ).toNgsiLdEntity()

        assertTrue(
            listOf("relation1", "relation2").toListOfUri().containsAll(expandedEntity.getLinkedEntitiesIds())
        )
    }
}
