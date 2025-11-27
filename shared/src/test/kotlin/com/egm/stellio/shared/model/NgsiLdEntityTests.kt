package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils.expandAttributes
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.UriUtils.toUri
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedAndResult
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.ZonedDateTime

class NgsiLdEntityTests {

    @Test
    fun `it should parse a minimal entity`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS)

        assertEquals("urn:ngsi-ld:Device:01234".toUri(), ngsiLdEntity.id)
        assertEquals(listOf("https://uri.etsi.org/ngsi-ld/default-context/Device"), ngsiLdEntity.types)
    }

    @Test
    fun `it should not parse an entity without an id`() = runTest {
        val rawEntity =
            """
            {
                "type": "Device"
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("The provided NGSI-LD entity does not contain an id property", it.message)
        }
    }

    @Test
    fun `it should not parse an entity without a type`() = runTest {
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

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("The provided NGSI-LD entity does not contain a type property", it.message)
        }
    }

    @Test
    fun `it should not parse an entity without an unknown attribute type`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "deviceState": {
                    "type": "UnknownProperty",
                    "value": 23
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Attribute ${NGSILD_DEFAULT_VOCAB}deviceState has an unknown type: " +
                    "https://uri.etsi.org/ngsi-ld/default-context/UnknownProperty",
                it.message
            )
        }
    }

    @Test
    fun `it should parse an entity with a minimal property`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        assertEquals(1, ngsiLdEntity.properties.size)
        val ngsiLdProperty = ngsiLdEntity.properties[0]
        assertEquals("${NGSILD_DEFAULT_VOCAB}deviceState", ngsiLdProperty.name)
        assertEquals(1, ngsiLdProperty.instances.size)
        val ngsiLdPropertyInstance = ngsiLdProperty.instances[0]
        assertEquals("Open", ngsiLdPropertyInstance.value)
    }

    @Test
    fun `it should parse an entity with a property having a JSON object value`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        assertEquals(1, ngsiLdEntity.properties.size)
        val ngsiLdProperty = ngsiLdEntity.properties[0]
        assertEquals("${NGSILD_DEFAULT_VOCAB}deviceState", ngsiLdProperty.name)
        assertEquals(1, ngsiLdProperty.instances.size)
        val ngsiLdPropertyInstance = ngsiLdProperty.instances[0]
        assertTrue(ngsiLdPropertyInstance.value is Map<*, *>)
        val valueMap = ngsiLdPropertyInstance.value as Map<String, *>
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
    fun `it should parse an entity with a property having all core fields`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val ngsiLdPropertyInstance = ngsiLdEntity.properties[0].instances[0]
        assertEquals("Open", ngsiLdPropertyInstance.value)
        assertEquals("MTR", ngsiLdPropertyInstance.unitCode)
        assertEquals("urn:ngsi-ld:Dataset:01234".toUri(), ngsiLdPropertyInstance.datasetId)
        assertEquals(ZonedDateTime.parse("2020-07-19T00:00:00Z"), ngsiLdPropertyInstance.observedAt)
    }

    @Test
    fun `it should not parse an entity with a property without a value`() = runTest {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "deviceState": {
                    "type": "Property"
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Property ${NGSILD_DEFAULT_VOCAB}deviceState has an instance without a value",
                it.message
            )
        }
    }

    @Test
    fun `it should parse an entity with a multi-attribute property`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        assertEquals(1, ngsiLdEntity.properties.size)
        val ngsiLdProperty = ngsiLdEntity.properties[0]
        assertEquals(2, ngsiLdProperty.instances.size)
        assertArrayEquals(
            arrayOf("urn:ngsi-ld:Dataset:01234".toUri(), "urn:ngsi-ld:Dataset:45678".toUri()),
            ngsiLdProperty.instances.map { it.datasetId }.toTypedArray()
        )
    }

    @Test
    fun `it should not parse a property with different type instances`() = runTest {
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

        expandAttributes(rawProperty, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdAttributes().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Attribute ${NGSILD_DEFAULT_VOCAB}deviceState can't have instances with different types",
                it.message
            )
        }
    }

    @Test
    fun `it should not parse a property with a duplicated datasetId`() = runTest {
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

        expandAttributes(rawProperty, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdAttributes().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Attribute ${NGSILD_DEFAULT_VOCAB}deviceState " +
                    "can't have more than one instance with the same datasetId",
                it.message
            )
        }
    }

    @Test
    fun `it should not parse a property with more than one default instance`() = runTest {
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

        expandAttributes(rawProperty, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdAttributes().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Attribute ${NGSILD_DEFAULT_VOCAB}deviceState can't have more than one default instance",
                it.message
            )
        }
    }

    @Test
    fun `it should parse a property with a datasetId`() = runTest {
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

        val ngsiLdAttributes =
            expandAttributes(rawProperty, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdAttributes().shouldSucceedAndResult()

        assertEquals(1, ngsiLdAttributes.size)
        val ngsiLdAttribute = ngsiLdAttributes[0]
        assertTrue(ngsiLdAttribute is NgsiLdProperty)
        val ngsiLdPropertyInstance = (ngsiLdAttribute as NgsiLdProperty).instances[0]
        assertEquals("urn:ngsi-ld:Dataset:fishName:1".toUri(), ngsiLdPropertyInstance.datasetId)
        assertEquals(35, ngsiLdPropertyInstance.value)
    }

    @Test
    fun `it should parse an entity with a minimal relationship`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        assertEquals(1, ngsiLdEntity.relationships.size)
        val ngsiLdRelationship = ngsiLdEntity.relationships[0]
        assertEquals("https://uri.etsi.org/ngsi-ld/default-context/refDeviceModel", ngsiLdRelationship.name)
        assertEquals(1, ngsiLdRelationship.instances.size)
        val ngsiLdRelationshipInstance = ngsiLdRelationship.instances[0]
        assertEquals("urn:ngsi-ld:DeviceModel:09876".toUri(), ngsiLdRelationshipInstance.objectId)
    }

    @Test
    fun `it should parse an entity with a relationship having all core fields`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val ngsiLdRelationshipInstance = ngsiLdEntity.relationships[0].instances[0]
        assertEquals("urn:ngsi-ld:DeviceModel:09876".toUri(), ngsiLdRelationshipInstance.objectId)
        assertEquals("urn:ngsi-ld:Dataset:01234".toUri(), ngsiLdRelationshipInstance.datasetId)
        assertEquals(ZonedDateTime.parse("2020-07-19T00:00:00Z"), ngsiLdRelationshipInstance.observedAt)
    }

    @Test
    fun `it should parse an entity with a multi-attribute relationship`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        assertEquals(1, ngsiLdEntity.relationships.size)
        val ngsiLdRelationship = ngsiLdEntity.relationships[0]
        assertEquals(2, ngsiLdRelationship.instances.size)
    }

    @Test
    fun `it should not parse a relationship with more than one default instance`() = runTest {
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

        expandAttributes(rawRelationship, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdAttributes().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Attribute ${NGSILD_DEFAULT_VOCAB}refDeviceModel can't have more " +
                    "than one default instance",
                it.message
            )
        }
    }

    @Test
    fun `it should not parse a relationship with a duplicated datasetId`() = runTest {
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

        expandAttributes(rawRelationship, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdAttributes().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Attribute ${NGSILD_DEFAULT_VOCAB}refDeviceModel " +
                    "can't have more than one instance with the same datasetId",
                it.message
            )
        }
    }

    @Test
    fun `it should not parse an attribute with different type instances`() = runTest {
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

        expandAttributes(rawRelationship, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdAttributes().shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Attribute ${NGSILD_DEFAULT_VOCAB}refDeviceModel can't have instances with different types",
                it.message
            )
        }
    }

    @Test
    fun `it should parse an entity with a Polygon location`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val location = ngsiLdEntity.geoProperties.find { it.name == NGSILD_LOCATION_IRI }
        assertNotNull(location)
        assertEquals("https://uri.etsi.org/ngsi-ld/location", location?.name)
        assertEquals(1, location?.instances?.size)
        val locationInstance = location?.instances?.get(0)
        assertEquals("POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))", locationInstance?.coordinates?.value)
    }

    @Test
    fun `it should parse an entity with a MultiPolygon location`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val location = ngsiLdEntity.geoProperties.find { it.name == NGSILD_LOCATION_IRI }
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
    fun `it should parse an entity with a Point location`() = runTest {
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

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val location = ngsiLdEntity.geoProperties.find { it.name == NGSILD_LOCATION_IRI }
        assertNotNull(location)
        assertEquals("https://uri.etsi.org/ngsi-ld/location", location?.name)
        assertEquals(1, location?.instances?.size)
        val locationInstance = location?.instances?.get(0)
        assertEquals("POINT (24.30623 60.07966)", locationInstance?.coordinates?.value)
    }

    @Test
    fun `it should parse an entity with a JsonProperty having a JSON object as a value`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "json": {
                        "address": "Parc des Princes",
                        "city": "Paris"
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val jsonProperty = ngsiLdEntity.jsonProperties.first()
        assertNotNull(jsonProperty)
        assertEquals("${NGSILD_DEFAULT_VOCAB}jsonProperty", jsonProperty.name)
        assertEquals(1, jsonProperty.instances.size)
        val jsonPropertyInstance = jsonProperty.instances[0]
        assertEquals(
            mapOf(
                "address" to "Parc des Princes",
                "city" to "Paris"
            ),
            jsonPropertyInstance.json
        )
    }

    @Test
    fun `it should parse an entity with a JsonProperty having an array of JSON objects as a value`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "json": [
                        { "key1": "value1" },
                        { "key2": "value2" }
                    ]
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val jsonProperty = ngsiLdEntity.jsonProperties.first()
        assertNotNull(jsonProperty)
        assertEquals("${NGSILD_DEFAULT_VOCAB}jsonProperty", jsonProperty.name)
        assertEquals(1, jsonProperty.instances.size)
        val jsonPropertyInstance = jsonProperty.instances[0]
        assertEquals(
            listOf(
                mapOf("key1" to "value1"),
                mapOf("key2" to "value2")
            ),
            jsonPropertyInstance.json
        )
    }

    @Test
    fun `it should parse an entity with a multi-attribute JsonProperty`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "jsonProperty": [{
                    "type": "JsonProperty",
                    "json": {
                        "address": "Parc des Princes",
                        "city": "Paris"
                    },
                    "datasetId": "urn:ngsi-ld:dataset:Parc-des-Princes"
                }, {
                    "type": "JsonProperty",
                    "json": {
                        "address": "Stade de la Beaujoire",
                        "city": "Nantes"
                    },
                    "datasetId": "urn:ngsi-ld:dataset:Stade-de-la-Beaujoire"
                }]
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val jsonProperty = ngsiLdEntity.jsonProperties.first()
        assertEquals(2, jsonProperty.instances.size)
    }

    @Test
    fun `it should parse an entity with a JsonProperty having JSON-LD reserved names as keys`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "json": {
                        "id": "Parc-des-Princes",
                        "type": "Stadium"
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val jsonPropertyInstance = ngsiLdEntity.jsonProperties.first().instances[0]
        assertEquals(
            mapOf(
                "id" to "Parc-des-Princes",
                "type" to "Stadium"
            ),
            jsonPropertyInstance.json
        )
    }

    @Test
    fun `it should parse an entity with a JsonProperty having a null value in its JSON map`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "json": {
                        "aNotNullMember": "notNull",
                        "aNullMember": null
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val jsonPropertyInstance = ngsiLdEntity.jsonProperties.first().instances[0]
        assertEquals(
            mapOf(
                "aNotNullMember" to "notNull",
                "aNullMember" to null
            ),
            jsonPropertyInstance.json
        )
    }

    @Test
    fun `it should not parse an entity with a JsonProperty without a json member`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "value": {
                        "id": "Parc-des-Princes"
                    }
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "JsonProperty ${NGSILD_DEFAULT_VOCAB}jsonProperty has an instance without a json member",
                    it.message
                )
            }
    }

    @Test
    fun `it should not parse an entity with a JsonProperty having a forbidden JSON type as value`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "jsonProperty": {
                    "type": "JsonProperty",
                    "json": "Parc-des-Princes"
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "JsonProperty ${NGSILD_DEFAULT_VOCAB}jsonProperty has a json member that is not a JSON object, " +
                        "nor an array of JSON objects",
                    it.message
                )
            }
    }

    @Test
    fun `it should parse an entity with a LanguageProperty`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "fr": ["Grand Place", "Grande Place"],
                        "nl": "Grote Markt",
                        "@none": "Big Place"
                    }
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val languageProperty = ngsiLdEntity.languageProperties.first()
        assertNotNull(languageProperty)
        assertEquals("${NGSILD_DEFAULT_VOCAB}languageProperty", languageProperty.name)
        assertEquals(1, languageProperty.instances.size)
        val languagePropertyInstance = languageProperty.instances[0]
        assertEquals(
            listOf(
                mapOf(
                    "@language" to "fr",
                    "@value" to "Grand Place"
                ),
                mapOf(
                    "@language" to "fr",
                    "@value" to "Grande Place"
                ),
                mapOf(
                    "@language" to "nl",
                    "@value" to "Grote Markt"
                ),
                mapOf(
                    "@value" to "Big Place"
                )
            ),
            languagePropertyInstance.languageMap
        )
    }

    @Test
    fun `it should not parse an entity with a LanguageProperty without a languageMap member`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "value": {
                        "id": "Parc-des-Princes"
                    }
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "LanguageProperty ${NGSILD_DEFAULT_VOCAB}languageProperty has an instance " +
                        "without a languageMap member",
                    it.message
                )
            }
    }

    @Test
    fun `it should not parse an entity with a LanguageProperty with an invalid languageMap member`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "languageMap": [{
                        "fr": "Grand Place",
                        "nl": "Grote Markt"
                    }]
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "LanguageProperty ${NGSILD_DEFAULT_VOCAB}languageProperty has an invalid languageMap member",
                    it.message
                )
            }
    }

    @Test
    fun `it should not parse an entity with a LanguageProperty with an invalid language tag`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "123": "Grand Place"
                    }
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "LanguageProperty ${NGSILD_DEFAULT_VOCAB}languageProperty has an invalid languageMap member",
                    it.message
                )
            }
    }

    @Test
    fun `it should not parse an entity with a LanguageProperty with an invalid value for a language`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "languageProperty": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "fr": {
                            "key": "Grand Place"
                        }
                    }
                }
            }
            """.trimIndent()

        assertThrows<BadRequestDataException> {
            expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS)
        }
    }

    @Test
    fun `it should parse an entity with a VocabProperty - array of strings value`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "vocabProperty": {
                    "type": "VocabProperty",
                    "vocab": ["stellio", "egm"]
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val vocabProperty = ngsiLdEntity.vocabProperties.first()
        assertNotNull(vocabProperty)
        assertEquals("${NGSILD_DEFAULT_VOCAB}vocabProperty", vocabProperty.name)
        assertEquals(1, vocabProperty.instances.size)
        val vocabPropertyInstance = vocabProperty.instances[0]
        assertEquals(
            listOf(
                mapOf(JSONLD_ID_KW to "${NGSILD_DEFAULT_VOCAB}stellio"),
                mapOf(JSONLD_ID_KW to "${NGSILD_DEFAULT_VOCAB}egm")
            ),
            vocabPropertyInstance.vocab
        )
    }

    @Test
    fun `it should parse an entity with a VocabProperty - string value`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "vocabProperty": {
                    "type": "VocabProperty",
                    "vocab": "stellio"
                }
            }
            """.trimIndent()

        val ngsiLdEntity = expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldSucceedAndResult()

        val vocabProperty = ngsiLdEntity.vocabProperties.first()
        assertNotNull(vocabProperty)
        assertEquals("${NGSILD_DEFAULT_VOCAB}vocabProperty", vocabProperty.name)
        assertEquals(1, vocabProperty.instances.size)
        val vocabPropertyInstance = vocabProperty.instances[0]
        assertEquals(
            listOf(
                mapOf(JSONLD_ID_KW to "${NGSILD_DEFAULT_VOCAB}stellio")
            ),
            vocabPropertyInstance.vocab
        )
    }

    @Test
    fun `it should not parse an entity with a VocabProperty without a vocab member`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "vocabProperty": {
                    "type": "VocabProperty",
                    "value": "stellio"
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "VocabProperty ${NGSILD_DEFAULT_VOCAB}vocabProperty has an instance " +
                        "without a vocab member",
                    it.message
                )
            }
    }

    @Test
    fun `it should not parse an entity with a VocabProperty with an invalid vocab member`() = runTest {
        val rawEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "vocabProperty": {
                    "type": "VocabProperty",
                    "vocab": {
                        "name": "stellio",
                        "company": "EGM"
                    }
                }
            }
            """.trimIndent()

        expandJsonLdEntity(rawEntity, NGSILD_TEST_CORE_CONTEXTS).toNgsiLdEntity()
            .shouldFail {
                assertInstanceOf(BadRequestDataException::class.java, it)
                assertEquals(
                    "VocabProperty ${NGSILD_DEFAULT_VOCAB}vocabProperty has a vocab member " +
                        "that is not a string, nor an array of string",
                    it.message
                )
            }
    }
}
