package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.http.MediaType

class ExpandedEntityTests {

    private val normalizedJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": {
                "type": "Property",
                "value": "Mercedes"
            },
            "isParked": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
                "observedAt": "2017-07-29T12:00:04Z",
                "providedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Person:Bob"
                }
            },
           "location": {
              "type": "GeoProperty",
              "value": {
                 "type": "Point",
                 "coordinates": [
                    24.30623,
                    60.07966
                 ]
              }
           },
            "@context": [
                "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            ]
        }
        """.trimIndent()

    private val simplifiedJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": "Mercedes",
            "isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
            "location": {
             "type": "Point",
             "coordinates": [
                24.30623,
                60.07966
             ]
           },
            "@context": [
                "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                "https://example.org/ngsi-ld/latest/parking.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
            ]
        }
        """.trimIndent()

    private val normalizedMultiAttributeJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": [
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "value": 10
                },
                {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:02",
                    "value": 11
                }
            ],
            "hasOwner": [
                {
                    "type": "Relationship",
                    "datasetId": "urn:ngsi-ld:Dataset:01",
                    "object": "urn:ngsi-ld:Person:John"
                },
                {
                    "type": "Relationship",
                    "datasetId": "urn:ngsi-ld:Dataset:02",
                    "object": "urn:ngsi-ld:Person:Jane"
                }
            ]
        }
        """.trimIndent()

    private val simplifiedMultiAttributeJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "speed": [ 10, 11 ],
            "hasOwner": [ "urn:ngsi-ld:Person:John", "urn:ngsi-ld:Person:Jane" ]
        }
        """.trimIndent()

    @Test
    fun `it should find an expanded attribute contained in the entity`() {
        val expandedEntity = ExpandedEntity(
            mapOf(INCOMING_PROPERTY to "", OUTGOING_PROPERTY to ""),
            NGSILD_TEST_CORE_CONTEXTS
        )

        val checkResult = expandedEntity.checkContainsAnyOf(setOf(TEMPERATURE_PROPERTY, INCOMING_PROPERTY))

        checkResult.fold({
            fail("it should have found one of the requested attributes")
        }, {})
    }

    @Test
    fun `it should not find an expanded attribute contained in the entity`() {
        val expandedEntity = ExpandedEntity(
            mapOf(INCOMING_PROPERTY to "", OUTGOING_PROPERTY to "", JSONLD_ID to "urn:ngsi-ld:Entity:01"),
            NGSILD_TEST_CORE_CONTEXTS
        )

        val checkResult = expandedEntity.checkContainsAnyOf(setOf(TEMPERATURE_PROPERTY, NGSILD_NAME_PROPERTY))

        checkResult.fold({
            assertEquals(
                "Entity urn:ngsi-ld:Entity:01 does not exist or it has none of the requested attributes : " +
                    "[https://ontology.eglobalmark.com/apic#temperature, https://schema.org/name]",
                it.message
            )
        }, {
            fail("it should have not found any of the requested attributes")
        })
    }

    @Test
    fun `it should get the attributes from a JSON-LD entity`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val expandedAttributes = expandJsonLdEntity(entity).getAttributes()
        assertThat(expandedAttributes)
            .hasSize(1)
            .containsKey(NGSILD_NAME_PROPERTY)
    }

    @Test
    fun `it should get the scopes from a JSON-LD entity`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "scope": ["/Nantes", "/Irrigation"],
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val scopes = expandJsonLdEntity(entity).getScopes()
        assertThat(scopes)
            .hasSize(2)
            .contains("/Nantes", "/Irrigation")
    }

    @Test
    fun `it should populate the createdAt information at root and attribute levels`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val jsonLdEntity = expandJsonLdEntity(entity).populateCreationTimeDate(ngsiLdDateTime())
        assertThat(jsonLdEntity.members).containsKey(NGSILD_CREATED_AT_PROPERTY)
        val nameAttributeInstances = jsonLdEntity.members[NGSILD_NAME_PROPERTY] as ExpandedAttributeInstances
        assertThat(nameAttributeInstances).hasSize(1)
        assertThat(nameAttributeInstances[0]).containsKey(NGSILD_CREATED_AT_PROPERTY)
    }

    @Test
    fun `it should simplify a compacted entity`() {
        val normalizedMap = normalizedJson.deserializeAsMap()
        val simplifiedMap = simplifiedJson.deserializeAsMap()

        val resultMap = normalizedMap.toKeyValues()

        assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should simplify a compacted entity with multi-attributes`() {
        val normalizedMap = normalizedMultiAttributeJson.deserializeAsMap()
        val simplifiedMap = simplifiedMultiAttributeJson.deserializeAsMap()

        val resultMap = normalizedMap.toKeyValues()

        assertEquals(simplifiedMap, resultMap)
    }

    @Test
    fun `it should return a simplified entity with sysAttrs`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "createdAt": "2023-11-25T08:00:00Z",
                "modifiedAt": "2023-11-25T09:00:00Z",
                "brandName": {
                    "type": "Property",
                    "value": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "createdAt": "2023-11-25T08:00:00Z",
                "modifiedAt": "2023-11-25T09:00:00Z",
                "brandName": "Mercedes"
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = true
            )
        )

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a simplified entity without sysAttrs`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "createdAt": "2023-11-25T08:00:00Z",
                "modifiedAt": "2023-11-25T09:00:00Z",
                "brandName": {
                    "type": "Property",
                    "value": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "brandName": "Mercedes"
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.forMediaType(MediaType.APPLICATION_JSON),
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false
            )
        )

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a normalized GeoJSON entity on location attribute`() {
        val inputEntity = normalizedJson.deserializeAsMap()

        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Feature",
                "geometry": {
                     "type": "Point",
                     "coordinates": [ 24.30623, 60.07966 ]
                },
                "properties": {
                    "type": "Vehicle",
                    "brandName": {
                        "type": "Property",
                        "value": "Mercedes"
                    },
                    "isParked": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
                        "observedAt": "2017-07-29T12:00:04Z",
                        "providedBy": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:Person:Bob"
                        }
                    },
                   "location": {
                      "type": "GeoProperty",
                      "value": {
                         "type": "Point",
                         "coordinates": [
                            24.30623,
                            60.07966
                         ]
                      }
                   },
                    "@context": [
                        "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                        "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                        "https://example.org/ngsi-ld/latest/parking.jsonld",
                        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                    ]
                }
            }
            """.trimIndent().deserializeAsMap()

        val actualEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.NORMALIZED,
                includeSysAttrs = false,
                geometryProperty = NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntity, actualEntity)
    }

    @Test
    fun `it should return a simplified GeoJSON entity on location attribute`() {
        val inputEntity = normalizedJson.deserializeAsMap()

        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Feature",
                "geometry": {
                     "type": "Point",
                     "coordinates": [ 24.30623, 60.07966 ]
                },
                "properties": {
                    "type": "Vehicle",
                    "brandName": "Mercedes",
                    "isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
                    "location": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    },
                    "@context": [
                        "https://example.org/ngsi-ld/latest/commonTerms.jsonld",
                        "https://example.org/ngsi-ld/latest/vehicle.jsonld",
                        "https://example.org/ngsi-ld/latest/parking.jsonld",
                        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                    ]
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                geometryProperty = NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return a GeoJSON entity with a null geometry if the GeoProperty does not exist`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "brandName": {
                    "type": "Property",
                    "value": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()

        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Feature",
                "geometry": null,
                "properties": {
                    "type": "Vehicle",
                    "brandName": "Mercedes"
                }
            }
            """.trimIndent().deserializeAsMap()

        val simplifiedEntity = inputEntity.toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                geometryProperty = NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntity, simplifiedEntity)
    }

    @Test
    fun `it should return simplified GeoJSON entities`() {
        val inputEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    }
                }
            }
            """.trimIndent().deserializeAsMap()

        val expectedEntities =
            """
            {
                "type": "FeatureCollection",
                "features": [{
                    "id": "urn:ngsi-ld:Vehicle:A4567",
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    },
                    "properties": {
                        "type": "Vehicle",
                        "location": {
                            "type": "Point",
                            "coordinates": [ 24.30623, 60.07966 ]
                        }
                    }
                }, {
                    "id": "urn:ngsi-ld:Vehicle:A4567",
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [ 24.30623, 60.07966 ]
                    },
                    "properties": {
                        "type": "Vehicle",
                        "location": {
                            "type": "Point",
                            "coordinates": [ 24.30623, 60.07966 ]
                        }                        
                    }
                }]
            }
            """.trimIndent().deserializeAsMap()

        val actualEntities = listOf(inputEntity, inputEntity).toFinalRepresentation(
            NgsiLdDataRepresentation(
                EntityRepresentation.GEO_JSON,
                AttributeRepresentation.SIMPLIFIED,
                includeSysAttrs = false,
                NGSILD_LOCATION_TERM
            )
        )

        assertEquals(expectedEntities, actualEntities)
    }
}
