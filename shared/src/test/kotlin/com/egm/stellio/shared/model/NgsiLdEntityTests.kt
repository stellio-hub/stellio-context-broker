package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.ZonedDateTime

class NgsiLdEntityTests {

    private val targetRelationshipUri = "urn:ngsi-ld:Entity:target".toUri()
    private val targetRelationshipUri2 = "urn:ngsi-ld:Entity:target2".toUri()

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
    fun `it should not parse an entity with an invalid type name`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Invalid(Type)"
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()
        }
        assertEquals(
            "The provided NGSI-LD entity has a type with invalid characters",
            exception.message
        )
    }

    @Test
    fun `it should parse an entity with allowed characters for attribute name`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Property",
                "prefix:device_state": {
                    "type": "Property",
                    "value": 23
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()
        assertEquals(1, ngsiLdEntity.properties.size)
        assertEquals("prefix:device_state", ngsiLdEntity.properties[0].compactName)
    }

    @Test
    fun `it should not parse an entity with an invalid attribute name`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "device<State": {
                    "type": "Property",
                    "value": 23
                }
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()
        }
        assertEquals(
            "Entity has an invalid attribute name: device<State",
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
        assertNull(ngsiLdPropertyInstance.createdAt)
        assertNull(ngsiLdPropertyInstance.modifiedAt)
    }

    @Test
    fun `it should parse an entity with a property whose name contains a colon`() {
        val rawEntity =
            """
            {
              "id": "urn:ngsi-ld:Device:01234",
              "type": "Device",
              "prefix:name": {
                "type": "Property",
                "value": "Open"
              }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val ngsiLdProperty = ngsiLdEntity.properties[0]
        assertEquals("prefix:name", ngsiLdProperty.name)
    }

    @Test
    fun `it should parse an entity with a property having a JSON object value`() {
        val rawEntity =
            """
            {
              "id": "urn:ngsi-ld:Device:01234",
              "type": "Device",
              "deviceState": {
                "type": "Property",
                "value": {
                  "state1": "open",
                  "state2": "closed"
                }
              }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        assertEquals(1, ngsiLdEntity.properties.size)
        val ngsiLdProperty = ngsiLdEntity.properties[0]
        assertEquals("https://uri.fiware.org/ns/data-models#deviceState", ngsiLdProperty.name)
        assertEquals(1, ngsiLdProperty.instances.size)
        val ngsiLdPropertyInstance = ngsiLdProperty.instances[0]
        assertTrue(ngsiLdPropertyInstance.value is Map<*, *>)
        val valueMap = ngsiLdPropertyInstance.value as Map<String, String>
        assertEquals(2, valueMap.size)
        assertEquals(
            setOf(
                "https://uri.etsi.org/ngsi-ld/default-context/state1",
                "https://uri.etsi.org/ngsi-ld/default-context/state2"
            ),
            valueMap.keys
        )
        assertEquals(
            listOf(mapOf(JSONLD_VALUE_KW to "open")),
            valueMap["https://uri.etsi.org/ngsi-ld/default-context/state1"]
        )
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
    fun `it should parse an entity with a property having createdAt and modifiedAt information`() {
        val rawEntity =
            """
            {
              "id": "urn:ngsi-ld:Device:01234",
              "type": "Device",
              "deviceState": {
                "type": "Property",
                "value": "Open",
                "createdAt": "2022-01-19T00:00:00Z",
                "modifiedAt": "2022-01-29T00:00:00Z"
              }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val ngsiLdPropertyInstance = ngsiLdEntity.properties[0].instances[0]
        assertEquals(ZonedDateTime.parse("2022-01-19T00:00:00Z"), ngsiLdPropertyInstance.createdAt)
        assertEquals(ZonedDateTime.parse("2022-01-29T00:00:00Z"), ngsiLdPropertyInstance.modifiedAt)
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
            "Attribute https://uri.fiware.org/ns/data-models#deviceState " +
                "can't have more than one instance with the same datasetId",
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
        assertNull(ngsiLdRelationshipInstance.createdAt)
        assertNull(ngsiLdRelationshipInstance.modifiedAt)
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
                        "coordinates": [[
                            [100.0, 0.0],
                            [101.0, 0.0], 
                            [101.0, 1.0],
                            [100.0, 1.0],
                            [100.0, 0.0]
                        ]]
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val location = ngsiLdEntity.getGeoProperty("https://uri.etsi.org/ngsi-ld/location")
        assertNotNull(location)
        assertEquals("https://uri.etsi.org/ngsi-ld/location", location?.name)
        assertEquals(1, location?.instances?.size)
        val locationInstance = location?.instances?.get(0)
        assertEquals("POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))", locationInstance?.coordinates?.value)
        assertNull(locationInstance?.createdAt)
        assertNull(locationInstance?.modifiedAt)
    }

    @Test
    fun `it should parse an entity with a MultiPolygon location`() {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "MultiPolygon",
                        "coordinates": [[
                            [
                                [703459.5, 114441.8],
                                [703494.3, 114477.6],
                                [703709.8, 114268.8],
                                [703675, 114232.8],
                                [703459.5, 114441.8]
                            ],
                            [
                                [703458.5, 114441.8],
                                [703494.3, 114477.6],
                                [703709.8, 114268.8],
                                [703675, 114232.8],
                                [703458.5, 114441.8]
                            ]
                        ]] 
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, DEFAULT_CONTEXTS).toNgsiLdEntity()

        val location = ngsiLdEntity.getGeoProperty("https://uri.etsi.org/ngsi-ld/location")
        assertNotNull(location)
        assertEquals(1, location?.instances?.size)
        assertEquals("https://uri.etsi.org/ngsi-ld/location", location?.name)
        val locationInstance = location?.instances?.get(0)
        assertEquals(
            """
            MULTIPOLYGON ((
            (703459.5 114441.8, 703494.3 114477.6, 703709.8 114268.8, 703675 114232.8, 703459.5 114441.8), 
            (703458.5 114441.8, 703494.3 114477.6, 703709.8 114268.8, 703675 114232.8, 703458.5 114441.8)
            ))
            """.trimIndent().replace("\n", ""),
            locationInstance?.coordinates?.value
        )
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

        val location = ngsiLdEntity.getGeoProperty("https://uri.etsi.org/ngsi-ld/location")
        assertNotNull(location)
        assertEquals("https://uri.etsi.org/ngsi-ld/location", location?.name)
        assertEquals(1, location?.instances?.size)
        val locationInstance = location?.instances?.get(0)
        assertEquals("POINT (24.30623 60.07966)", locationInstance?.coordinates?.value)
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
                    "object": "$targetRelationshipUri"
                }
            }
            """.trimIndent(),
            DEFAULT_CONTEXTS
        ).toNgsiLdEntity()

        assertEquals(arrayListOf(targetRelationshipUri), expandedEntity.getLinkedEntitiesIds())
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
                    "object": "$targetRelationshipUri"
                 },
                 "speed": {
                    "type": "Property", 
                    "value": 35, 
                    "flashedFrom": { 
                        "type": "Relationship", 
                        "object": "$targetRelationshipUri2" 
                    }
                }
            }
            """.trimIndent(),
            DEFAULT_CONTEXTS
        ).toNgsiLdEntity()

        assertTrue(
            listOf(targetRelationshipUri, targetRelationshipUri2)
                .containsAll(expandedEntity.getLinkedEntitiesIds())
        )
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
                    "object": "$targetRelationshipUri",
                    "createdBy": {
                        "type": "Relationship",
                        "object": "$targetRelationshipUri2"
                    }
                }
            }
            """.trimIndent(),
            DEFAULT_CONTEXTS
        ).toNgsiLdEntity()

        assertTrue(
            listOf(targetRelationshipUri, targetRelationshipUri2)
                .containsAll(expandedEntity.getLinkedEntitiesIds())
        )
    }
}
