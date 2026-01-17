package com.egm.stellio.shared.model

import com.egm.stellio.shared.model.CompactedEntityFixtureData.normalizedEntity
import com.egm.stellio.shared.model.CompactedEntityFixtureData.normalizedMultiAttributeEntity
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CompactedEntityLinkedTests {

    @Test
    fun `it shoud extract a single-instance relationship`() {
        val relationships = normalizedEntity.getRelationshipsNamesWithObjects()

        assertThat(relationships)
            .hasSize(1)
            .containsEntry("isParked", setOf("urn:ngsi-ld:OffStreetParking:Downtown1".toUri()))
    }

    @Test
    fun `it shoud extract a multi-instance relationship`() {
        val relationships = normalizedMultiAttributeEntity.getRelationshipsNamesWithObjects()

        assertThat(relationships)
            .hasSize(1)
            .containsEntry("hasOwner", setOf("urn:ngsi-ld:Person:John".toUri(), "urn:ngsi-ld:Person:Jane".toUri()))
    }

    @Test
    fun `it shoud extract single and multi-instances of relationships`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "r1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01"
                },
                "r2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:02"
                },
                "r3": [{
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:03",
                    "datasetId": "urn:ngsi-ld:Dataset:03"
                }, {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:04",
                    "datasetId": "urn:ngsi-ld:Dataset:04"
                }]
            }
        """.trimIndent().deserializeAsMap()

        val relationships = compactedEntity.getRelationshipsNamesWithObjects()

        assertThat(relationships)
            .hasSize(3)
            .containsExactlyEntriesOf(
                mapOf(
                    "r1" to setOf("urn:ngsi-ld:LinkedEntity:01".toUri()),
                    "r2" to setOf("urn:ngsi-ld:LinkedEntity:02".toUri()),
                    "r3" to setOf("urn:ngsi-ld:LinkedEntity:03".toUri(), "urn:ngsi-ld:LinkedEntity:04".toUri())
                )
            )
    }

    @Test
    fun `it shoud extract all relationships without merging duplicate objects`() {
        val compactedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "r1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01"
                },
                "r2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01"
                }
            }
        """.trimIndent().deserializeAsMap()

        val relationships = compactedEntity.getRelationshipsNamesWithObjects()

        assertThat(relationships)
            .hasSize(2)
            .containsExactlyEntriesOf(
                mapOf(
                    "r1" to setOf("urn:ngsi-ld:LinkedEntity:01".toUri()),
                    "r2" to setOf("urn:ngsi-ld:LinkedEntity:01".toUri())
                )
            )
    }

    @Test
    fun `it shoud extract relationships from multiple entities`() {
        val compactedEntities = """
            [{
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "r1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01"
                },
                "r2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:02"
                }
            },{
                "id": "urn:ngsi-ld:Entity:02",
                "type": "Entity",
                "r1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:03"
                },
                "r2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:04"
                }
            }]
        """.trimIndent().deserializeAsList()

        val relationships = compactedEntities.getRelationshipsNamesWithObjects()

        assertThat(relationships)
            .hasSize(2)
            .containsExactlyEntriesOf(
                mapOf(
                    "r1" to setOf("urn:ngsi-ld:LinkedEntity:01".toUri(), "urn:ngsi-ld:LinkedEntity:03".toUri()),
                    "r2" to setOf("urn:ngsi-ld:LinkedEntity:02".toUri(), "urn:ngsi-ld:LinkedEntity:04".toUri())
                )
            )
    }

    @Test
    fun `it should inline an entity with two single instance relationhips`() = runTest {
        val linkingEntity = """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01"
                },
                "rel2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:02"
                }
            }
        """.trimIndent().deserializeAsMap()
        val linkedEntity01 = """
            {
                "id": "urn:ngsi-ld:LinkedEntity:01",
                "type": "LinkedEntity"
            }
        """.trimIndent().deserializeAsMap()
        val linkedEntity02 = """
            {
                "id": "urn:ngsi-ld:LinkedEntity:02",
                "type": "LinkedEntity"
            }
        """.trimIndent().deserializeAsMap()

        val inlinedEntity = linkingEntity.inlineLinkedEntities(
            mapOf(
                "urn:ngsi-ld:LinkedEntity:01" to linkedEntity01,
                "urn:ngsi-ld:LinkedEntity:02" to linkedEntity02
            )
        )

        assertJsonPayloadsAreEqual(
            """
                {
                    "id":"urn:ngsi-ld:LinkingEntity:01",
                    "type":"LinkingEntity",
                    "rel1":{
                        "type":"Relationship",
                        "object":"urn:ngsi-ld:LinkedEntity:01",
                        "entity":{
                            "id":"urn:ngsi-ld:LinkedEntity:01",
                            "type":"LinkedEntity"
                        }
                    },
                    "rel2":{
                        "type":"Relationship",
                        "object":"urn:ngsi-ld:LinkedEntity:02",
                        "entity":{
                            "id":"urn:ngsi-ld:LinkedEntity:02",
                            "type":"LinkedEntity"
                        }
                    }
                }
            """.trimIndent(),
            serializeObject(inlinedEntity)
        )
    }

    @Test
    fun `it should inline an entity with a multi-instance relationhip`() = runTest {
        val linkingEntity = """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel": [{
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01"
                }, {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:02"
                }]
            }
        """.trimIndent().deserializeAsMap()
        val linkedEntity01 = """
            {
                "id": "urn:ngsi-ld:LinkedEntity:01",
                "type": "LinkedEntity"
            }
        """.trimIndent().deserializeAsMap()
        val linkedEntity02 = """
            {
                "id": "urn:ngsi-ld:LinkedEntity:02",
                "type": "LinkedEntity"
            }
        """.trimIndent().deserializeAsMap()

        val inlinedEntity = linkingEntity.inlineLinkedEntities(
            mapOf(
                "urn:ngsi-ld:LinkedEntity:01" to linkedEntity01,
                "urn:ngsi-ld:LinkedEntity:02" to linkedEntity02
            )
        )

        assertJsonPayloadsAreEqual(
            """
                {
                    "id":"urn:ngsi-ld:LinkingEntity:01",
                    "type":"LinkingEntity",
                    "rel":[{
                        "type":"Relationship",
                        "object":"urn:ngsi-ld:LinkedEntity:01",
                        "entity":{
                            "id":"urn:ngsi-ld:LinkedEntity:01",
                            "type":"LinkedEntity"
                        }
                    },{
                        "type":"Relationship",
                        "object":"urn:ngsi-ld:LinkedEntity:02",
                        "entity":{
                            "id":"urn:ngsi-ld:LinkedEntity:02",
                            "type":"LinkedEntity"
                        }
                    }]
                }
            """.trimIndent(),
            serializeObject(inlinedEntity)
        )
    }

    @Test
    fun `it should inline two entities with their respective relationhips`() = runTest {
        val linkingEntities = """
            [{
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01"
                },
                "rel2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:02"
                }
            },{
                "id": "urn:ngsi-ld:LinkingEntity:02",
                "type": "LinkingEntity",
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:03"
                },
                "rel2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:04"
                }
            }]
        """.trimIndent().deserializeAsList()
        val linkedEntities = (1..4).map {
            mapOf(
                "urn:ngsi-ld:LinkedEntity:0$it" to
                    """
                    {
                        "id": "urn:ngsi-ld:LinkedEntity:0$it",
                        "type": "LinkedEntity"
                    }
                    """.trimIndent().deserializeAsMap()
            )
        }.fold(emptyMap<String, CompactedEntity>()) { acc, map -> acc.plus(map) }

        val inlinedEntity = linkingEntities.inlineLinkedEntities(linkedEntities)

        assertJsonPayloadsAreEqual(
            """
                [
                    {
                        "id": "urn:ngsi-ld:LinkingEntity:01",
                        "type": "LinkingEntity",
                        "rel1": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:01",
                            "entity": {
                                "id": "urn:ngsi-ld:LinkedEntity:01",
                                "type": "LinkedEntity"
                            }
                        },
                        "rel2": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:02",
                            "entity": {
                                "id": "urn:ngsi-ld:LinkedEntity:02",
                                "type": "LinkedEntity"
                            }
                        }
                    },
                    {
                        "id": "urn:ngsi-ld:LinkingEntity:02",
                        "type": "LinkingEntity",
                        "rel1": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:03",
                            "entity": {
                                "id": "urn:ngsi-ld:LinkedEntity:03",
                                "type": "LinkedEntity"
                            }
                        },
                        "rel2": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:04",
                            "entity": {
                                "id": "urn:ngsi-ld:LinkedEntity:04",
                                "type": "LinkedEntity"
                            }
                        }
                    }
                ]
            """.trimIndent(),
            serializeObject(inlinedEntity)
        )
    }

    @Test
    fun `it should remove sysAttrs from inlined entities`() = runTest {
        val inlineLinkingEntity = """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "createdAt": "2024-11-11T11:00:00Z",
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01",
                    "createdAt": "2024-11-11T11:00:00Z",
                    "entity": {
                        "id": "urn:ngsi-ld:LinkedEntity:01",
                        "type": "LinkedEntity",
                        "createdAt": "2024-11-11T11:00:00Z",
                        "prop1": {
                            "type": "Property",
                            "value": 1,
                            "createdAt": "2024-11-11T11:00:00Z"
                        }
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val inlinedEntity = inlineLinkingEntity.withoutSysAttrs(null)

        assertJsonPayloadsAreEqual(
            """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01",
                    "entity": {
                        "id": "urn:ngsi-ld:LinkedEntity:01",
                        "type": "LinkedEntity",
                        "prop1": {
                            "type": "Property",
                            "value": 1
                        }
                    }
                }
            }
            """.trimIndent(),
            serializeObject(inlinedEntity)
        )
    }

    @Test
    fun `it should filter language properties for inlined entities`() = runTest {
        val inlineLinkingEntity = """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "city": {
                    "type": "LanguageProperty",
                    "languageMap": {
                        "fr": "Nantes",
                        "br": "Naoned"
                    }
                },
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01",
                    "entity": {
                        "id": "urn:ngsi-ld:LinkedEntity:01",
                        "type": "LinkedEntity",
                        "langProp2": {
                            "type": "LanguageProperty",
                            "languageMap": {
                                "fr": "Grand Place",
                                "br": "Lec'h Bras"
                            }
                        }
                    }
                }
            }
        """.trimIndent().deserializeAsMap()

        val inlinedEntity = inlineLinkingEntity.toFilteredLanguageProperties("br")

        assertJsonPayloadsAreEqual(
            """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "city": {
                    "type": "Property",
                    "value": "Naoned",
                    "lang": "br"
                },
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01",
                    "entity": {
                        "id": "urn:ngsi-ld:LinkedEntity:01",
                        "type": "LinkedEntity",
                        "langProp2": {
                            "type": "Property",
                            "value": "Lec'h Bras",
                            "lang": "br"
                        }
                    }
                }
            }
            """.trimIndent(),
            serializeObject(inlinedEntity)
        )
    }

    @Test
    fun `it should simplify relationships for inlined entities`() = runTest {
        val inlineLinkingEntity = """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01",
                     "entity": {
                        "id": "urn:ngsi-ld:LinkedEntity:01",
                        "type": "LinkedEntity",
                        "name": "My linked entity 01"
                    }
                },
                "rel2": [
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:02",
                        "datasetId": "urn:ngsi-ld:Dataset:02",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:02",
                            "type": "LinkedEntity",
                            "name": "My linked entity 02"
                        }
                    },
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:03",
                        "datasetId": "urn:ngsi-ld:Dataset:03",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:03",
                            "type": "LinkedEntity",
                            "name": "My linked entity 03"
                        }
                    }
                ]
            }
        """.trimIndent().deserializeAsMap()

        val inlinedEntity = inlineLinkingEntity.toSimplifiedAttributes()

        assertJsonPayloadsAreEqual(
            """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel1": {
                    "id": "urn:ngsi-ld:LinkedEntity:01",
                    "type": "LinkedEntity",
                    "name": "My linked entity 01"
                },
                "rel2": {
                    "dataset": {
                        "urn:ngsi-ld:Dataset:02": {
                            "id": "urn:ngsi-ld:LinkedEntity:02",
                            "type": "LinkedEntity",
                            "name": "My linked entity 02"
                        },
                        "urn:ngsi-ld:Dataset:03": {
                            "id": "urn:ngsi-ld:LinkedEntity:03",
                            "type": "LinkedEntity",
                            "name": "My linked entity 03"
                        }
                    }
                }
            }
            """.trimIndent(),
            serializeObject(inlinedEntity)
        )
    }
}
