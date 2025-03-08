package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.JoinType
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.LINKED_ENTITY_COMPACT_TYPE
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadAndExpandMinimalEntity
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [LinkedEntityService::class])
@ActiveProfiles("test")
class LinkedEntityServiceTests {

    @Autowired
    private lateinit var linkedEntityService: LinkedEntityService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    private val linkingEntityWithTwoRelationships: CompactedEntity = """
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

    private val otherLinkingEntityWithTwoRelationships: CompactedEntity = """
        {
            "id": "urn:ngsi-ld:LinkingEntity:02",
            "type": "LinkingEntity",
            "rel1": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:LinkedEntity:01"
            },
            "rel2": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:LinkedEntity:03"
            }
        }
    """.trimIndent().deserializeAsMap()

    private val linkingEntityWithoutRelationships: CompactedEntity = """
        {
            "id": "urn:ngsi-ld:LinkingEntity:01",
            "type": "LinkingEntity",
            "prop1": {
                "type": "Property",
                "value": "Prop 1"
            }
        }
    """.trimIndent().deserializeAsMap()

    @Test
    fun `it should return the input entity if no join is specified`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            linkingEntityWithTwoRelationships,
            EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            null
        ).shouldSucceedAndResult()

        assertEquals(1, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(linkingEntityWithTwoRelationships),
            serializeObject(compactedEntities[0])
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any(), any<Sub>())
        }
    }

    @Test
    fun `it should return the input entity if @none join is specified`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            linkingEntityWithTwoRelationships,
            EntitiesQueryFromGet(
                linkedEntityQuery = LinkedEntityQuery(),
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            null
        ).shouldSucceedAndResult()

        assertEquals(1, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(linkingEntityWithTwoRelationships),
            serializeObject(compactedEntities[0])
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any(), any<Sub>())
        }
    }

    @Test
    fun `it should return the input entities if no join is specified`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships),
            EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            null
        ).shouldSucceedAndResult()

        assertEquals(2, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships)),
            serializeObject(compactedEntities)
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any(), any<Sub>())
        }
    }

    @Test
    fun `it should return only the input entity if it has no relationships`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            linkingEntityWithoutRelationships,
            EntitiesQueryFromGet(
                linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 1.toUInt()),
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            null
        ).shouldSucceedAndResult()

        assertEquals(1, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(listOf(linkingEntityWithoutRelationships)),
            serializeObject(compactedEntities)
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any(), any<Sub>())
        }
    }

    @Test
    fun `it should return an empty list if no entities are provided in the input`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            emptyList(),
            EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            null
        ).shouldSucceedAndResult()

        assertEquals(0, compactedEntities.size)

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any(), any<Sub>())
        }
    }

    @Test
    fun `it should flatten lists of linking and linked entities`() = runTest {
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

        val flattenedEntities =
            linkedEntityService.flattenLinkedEntities(
                listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships),
                listOf(linkedEntity01, linkedEntity02)
            )

        assertJsonPayloadsAreEqual(
            """
                [{
                    "id":"urn:ngsi-ld:LinkingEntity:01",
                    "type":"LinkingEntity",
                    "rel1": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:01"
                    },
                    "rel2": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:02"
                    }
                },
                {
                    "id": "urn:ngsi-ld:LinkingEntity:02",
                    "type": "LinkingEntity",
                    "rel1": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:01"
                    },
                    "rel2": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:03"
                    }
                },
                {
                    "id": "urn:ngsi-ld:LinkedEntity:01",
                    "type": "LinkedEntity"
                },
                {
                    "id": "urn:ngsi-ld:LinkedEntity:02",
                    "type": "LinkedEntity"
                }]
            """.trimIndent(),
            serializeObject(flattenedEntities)
        )
    }

    private fun prepareMockedAnswersForJoinLevel2OnEntity() {
        coEvery {
            entityQueryService.queryEntities(any(), any<Sub>())
        } coAnswers {
            Pair(
                listOf(
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:01",
                        attributes = """
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:01"
                            }
                        """.trimIndent()
                    ),
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:02",
                        attributes = """
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:02"
                            }
                        """.trimIndent()
                    )
                ),
                2
            ).right()
        } coAndThen {
            Pair(
                listOf(
                    loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:Level2:01"),
                    loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:Level2:02")
                ),
                2
            ).right()
        }
    }

    @Test
    fun `it should flatten an entity up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntity()

        val flattenedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                null
            ).shouldSucceedAndResult()

        assertJsonPayloadsAreEqual(
            """
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
                },
                {
                    "id": "urn:ngsi-ld:LinkedEntity:01",
                    "type": "LinkedEntity",
                    "rel3": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:Level2:01"
                    },
                    "@context": "$NGSILD_TEST_CORE_CONTEXT"
                },
                {
                    "id": "urn:ngsi-ld:LinkedEntity:02",
                    "type": "LinkedEntity",
                    "rel3": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:Level2:02"
                    },
                    "@context": "$NGSILD_TEST_CORE_CONTEXT"
                },
                {
                    "id": "urn:ngsi-ld:LinkedEntity:Level2:01",
                    "type": "LinkedEntity",
                    "@context": "$NGSILD_TEST_CORE_CONTEXT"
                },
                {
                    "id": "urn:ngsi-ld:LinkedEntity:Level2:02",
                    "type": "LinkedEntity",
                    "@context": "$NGSILD_TEST_CORE_CONTEXT"
                }]
            """.trimIndent(),
            serializeObject(flattenedEntities)
        )
    }

    @Test
    fun `it should inline an entity up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntity()

        val inlinedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                null
            ).shouldSucceedAndResult()

        assertJsonPayloadsAreEqual(
            """
                [{
                    "id": "urn:ngsi-ld:LinkingEntity:01",
                    "type": "LinkingEntity",
                    "rel1": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:01",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:01",
                            "type": "LinkedEntity",
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:01",
                                "entity": {
                                    "id": "urn:ngsi-ld:LinkedEntity:Level2:01",
                                    "type": "LinkedEntity"
                                }
                            }
                        }
                    },
                    "rel2": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:02",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:02",
                            "type": "LinkedEntity",
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:02",
                                "entity": {
                                    "id": "urn:ngsi-ld:LinkedEntity:Level2:02",
                                    "type": "LinkedEntity"
                                }
                            }
                        }
                    }
                }]
            """.trimIndent(),
            serializeObject(inlinedEntities)
        )
    }

    private fun prepareMockedAnswersForJoinLevel2OnEntities() {
        coEvery {
            entityQueryService.queryEntities(any(), any<Sub>())
        } coAnswers {
            Pair(
                listOf(
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:01",
                        attributes = """
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:01"
                            }
                        """.trimIndent()
                    ),
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:02",
                        attributes = """
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:02"
                            }
                        """.trimIndent()
                    ),
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:03",
                        attributes = """
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:03"
                            }
                        """.trimIndent()
                    )
                ),
                2
            ).right()
        } coAndThen {
            Pair(
                listOf(
                    loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:Level2:01"),
                    loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:Level2:02"),
                    loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:Level2:03")
                ),
                2
            ).right()
        }
    }

    @Test
    fun `it should flatten entities up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntities()

        val flattenedEntities =
            linkedEntityService.processLinkedEntities(
                listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships),
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                null
            ).shouldSucceedAndResult()

        val expectedEntitiesIds = setOf(
            "urn:ngsi-ld:LinkingEntity:01",
            "urn:ngsi-ld:LinkingEntity:02",
            "urn:ngsi-ld:LinkedEntity:01",
            "urn:ngsi-ld:LinkedEntity:02",
            "urn:ngsi-ld:LinkedEntity:03",
            "urn:ngsi-ld:LinkedEntity:Level2:01",
            "urn:ngsi-ld:LinkedEntity:Level2:02",
            "urn:ngsi-ld:LinkedEntity:Level2:03"
        )
        assertThat(flattenedEntities)
            .hasSize(8)
            .allMatch { expectedEntitiesIds.contains(it["id"]) }
    }

    @Test
    fun `it should inline entities up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntities()

        val inlinedEntities =
            linkedEntityService.processLinkedEntities(
                listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships),
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                null
            ).shouldSucceedAndResult()

        assertJsonPayloadsAreEqual(
            """
                [{
                    "id": "urn:ngsi-ld:LinkingEntity:01",
                    "type": "LinkingEntity",
                    "rel1": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:01",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:01",
                            "type": "LinkedEntity",
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:01",
                                "entity": {
                                    "id": "urn:ngsi-ld:LinkedEntity:Level2:01",
                                    "type": "LinkedEntity"
                                }
                            }
                        }
                    },
                    "rel2": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:02",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:02",
                            "type": "LinkedEntity",
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:02",
                                "entity": {
                                    "id": "urn:ngsi-ld:LinkedEntity:Level2:02",
                                    "type": "LinkedEntity"
                                }
                            }
                        }
                    }
                },
                {
                    "id": "urn:ngsi-ld:LinkingEntity:02",
                    "type": "LinkingEntity",
                    "rel1": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:01",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:01",
                            "type": "LinkedEntity",
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:01",
                                "entity": {
                                    "id": "urn:ngsi-ld:LinkedEntity:Level2:01",
                                    "type": "LinkedEntity"
                                }
                            }
                        }
                    },
                    "rel2": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:03",
                        "entity": {
                            "id": "urn:ngsi-ld:LinkedEntity:03",
                            "type": "LinkedEntity",
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:03",
                                "entity": {
                                    "id": "urn:ngsi-ld:LinkedEntity:Level2:03",
                                    "type": "LinkedEntity"
                                }
                            }
                        }
                    }
                }]
            """.trimIndent(),
            serializeObject(inlinedEntities)
        )
    }

    private suspend fun loadAndExpandMinimalLinkedEntity(id: String, attributes: String? = null): ExpandedEntity =
        loadAndExpandMinimalEntity(id, LINKED_ENTITY_COMPACT_TYPE, attributes)
}
