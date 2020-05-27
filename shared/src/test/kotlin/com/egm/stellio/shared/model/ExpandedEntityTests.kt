package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.NgsiLdParsingUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ExpandedEntityTests {

    @Test
    fun `it should get relationships from ExpandedEntity`() {
        val expandedEntity = NgsiLdParsingUtils.parseEntity(
            """
            { 
              "id": "urn:ngsi-ld:Vehicle:A12388", 
              "type": "Vehicle", 
              "connectsTo": { 
                "type": "Relationship",
                "object": "relation1"
              },
              "@context": [
                  "https://schema.lab.fiware.org/ld/context",
                  "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
              ]
            }
            """.trimIndent(),
            listOf()
        )

        assertEquals(arrayListOf("relation1"), expandedEntity.getLinkedEntitiesIds())
    }

    @Test
    fun `it should get relationships of properties from ExpandedEntity`() {
        val expandedEntity = NgsiLdParsingUtils.parseEntity(
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
                }, 
                "@context": [ 
                    "https://schema.lab.fiware.org/ld/context", 
                    "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" 
                ] 
            }
            """.trimIndent(),
            listOf()
        )

        assertEquals(listOf("Radar", "relation1"), expandedEntity.getLinkedEntitiesIds())
    }

    @Test
    fun `it should get relationships of Relations from ExpandedEntity`() {
        val expandedEntity = NgsiLdParsingUtils.parseEntity(
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
                },
                "@context": [ 
                    "https://schema.lab.fiware.org/ld/context", 
                    "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" 
                ]
            }
            """.trimIndent(),
            listOf()
        )

        assertEquals(listOf("relation1", "relation2"), expandedEntity.getLinkedEntitiesIds())
    }
}