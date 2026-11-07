package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.csr.CsrUtils.gimmeRawCSR
import com.egm.stellio.search.csr.model.MiscellaneousWarning
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.service.DistributedEntityConsumptionService
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.model.AttributeProjection.Companion.parsePickOmitParameters
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.JoinType
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.LINKED_ENTITY_TERM
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadAndExpandMinimalEntity
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [LinkedEntityService::class])
@ActiveProfiles("test")
class LinkedEntityServiceTests {

    @Autowired
    private lateinit var linkedEntityService: LinkedEntityService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var distributedEntityConsumptionService: DistributedEntityConsumptionService

    private val linkingEntityWithObjectTypes: CompactedEntity = """
        {
            "id": "urn:ngsi-ld:LinkingEntity:01",
            "type": "LinkingEntity",
            "localRel": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:LinkedEntity:01",
                "objectType": "LinkedEntity"
            },
            "remoteRel": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:LinkedEntity:02",
                "objectType": "LinkedEntity"
            },
            "untypedRel": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:LinkedEntity:03"
            }
        }
    """.trimIndent().deserializeAsMap()

    private val remoteLinkedEntity: CompactedEntity = """
        {
            "id": "urn:ngsi-ld:LinkedEntity:02",
            "type": "LinkedEntity",
            "name": {
                "type": "Property",
                "value": "remote"
            }
        }
    """.trimIndent().deserializeAsMap()

    private val inlineEntitiesQuery = EntitiesQueryFromGet(
        linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 1.toUInt()),
        paginationQuery = PaginationQuery(0, 100),
        contexts = NGSILD_TEST_CORE_CONTEXTS
    )

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
    fun `processLinkedEntities should return the input entity if no join is specified`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            linkingEntityWithTwoRelationships,
            EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            HttpHeaders()
        ).shouldSucceedAndResult().first

        assertEquals(1, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(linkingEntityWithTwoRelationships),
            serializeObject(compactedEntities[0])
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any())
        }
    }

    @Test
    fun `processLinkedEntities should return the input entity if @none join is specified`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            linkingEntityWithTwoRelationships,
            EntitiesQueryFromGet(
                linkedEntityQuery = LinkedEntityQuery(),
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            HttpHeaders()
        ).shouldSucceedAndResult().first

        assertEquals(1, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(linkingEntityWithTwoRelationships),
            serializeObject(compactedEntities[0])
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any())
        }
    }

    @Test
    fun `processLinkedEntities should return the input entities if no join is specified`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships),
            EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            HttpHeaders()
        ).shouldSucceedAndResult().first

        assertEquals(2, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships)),
            serializeObject(compactedEntities)
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any())
        }
    }

    @Test
    fun `processLinkedEntities should return only the input entity if it has no relationships`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            linkingEntityWithoutRelationships,
            EntitiesQueryFromGet(
                linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 1.toUInt()),
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            HttpHeaders()
        ).shouldSucceedAndResult().first

        assertEquals(1, compactedEntities.size)
        assertJsonPayloadsAreEqual(
            serializeObject(listOf(linkingEntityWithoutRelationships)),
            serializeObject(compactedEntities)
        )

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any())
        }
    }

    @Test
    fun `processLinkedEntities should return an empty list if no entities are provided in the input`() = runTest {
        val compactedEntities = linkedEntityService.processLinkedEntities(
            emptyList(),
            EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(0, 100),
                contexts = NGSILD_TEST_CORE_CONTEXTS
            ),
            HttpHeaders()
        ).shouldSucceedAndResult().first

        assertEquals(0, compactedEntities.size)

        coVerify(exactly = 0) {
            entityQueryService.queryEntities(any())
        }
    }

    @Test
    fun `processLinkedEntities should flatten an entity with a multivalued relationship`() = runTest {
        val linkingEntity = """
            {
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel": {
                    "type": "Relationship",
                    "object": [
                        "urn:ngsi-ld:LinkedEntity:01",
                        "urn:ngsi-ld:LinkedEntity:02"
                    ]
                }
            }
        """.trimIndent().deserializeAsMap()
        coEvery {
            entityQueryService.queryEntities(
                match<EntitiesQueryFromGet> {
                    it.ids == setOf(
                        "urn:ngsi-ld:LinkedEntity:01".toUri(),
                        "urn:ngsi-ld:LinkedEntity:02".toUri()
                    )
                }
            )
        } returns Pair(
            listOf(
                loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:01"),
                loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:02")
            ),
            2
        ).right()

        val flattenedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntity,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 1.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

        assertThat(flattenedEntities.map { it["id"] })
            .containsExactly(
                "urn:ngsi-ld:LinkingEntity:01",
                "urn:ngsi-ld:LinkedEntity:01",
                "urn:ngsi-ld:LinkedEntity:02"
            )
    }

    private fun prepareMockedAnswersForJoinLevel2OnEntity() {
        coEvery {
            entityQueryService.queryEntities(any())
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
    fun `processLinkedEntities should flatten an entity up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntity()

        val flattenedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

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
    fun `processLinkedEntities should inline an entity up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntity()

        val inlinedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

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
            entityQueryService.queryEntities(any())
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
    fun `processLinkedEntities should flatten entities up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntities()

        val flattenedEntities =
            linkedEntityService.processLinkedEntities(
                listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships),
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

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
    fun `processLinkedEntities should inline entities up to the asked 2nd level`() = runTest {
        prepareMockedAnswersForJoinLevel2OnEntities()

        val inlinedEntities =
            linkedEntityService.processLinkedEntities(
                listOf(linkingEntityWithTwoRelationships, otherLinkingEntityWithTwoRelationships),
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

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

    private fun prepareMockedAnswersForJoinLevelWithPickOnEntity() {
        coEvery {
            entityQueryService.queryEntities(any())
        } coAnswers {
            Pair(
                listOf(
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:01",
                        attributes = """
                            "title": {
                                "type": "Property",
                                "value": "Title of linked entity 01"
                            },
                            "description": {
                                "type": "Property",
                                "value": "Description of linked entity 01"
                            }                            
                        """.trimIndent()
                    ),
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:02",
                        attributes = """
                            "title": {
                                "type": "Property",
                                "value": "Title of linked entity 02"
                            }
                        """.trimIndent()
                    )
                ),
                2
            ).right()
        }
    }

    @Test
    fun `processLinkedEntities should flatten an entity and apply pick on relationships`() = runTest {
        prepareMockedAnswersForJoinLevelWithPickOnEntity()

        val flattenedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    pick = parsePickOmitParameters("rel1{id,title},rel2{id,type}", null)
                        .shouldSucceedAndResult().first,
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

        assertJsonPayloadsAreEqual(
            """
                [
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
                    },
                    {
                        "id": "urn:ngsi-ld:LinkedEntity:01",
                        "title": {
                            "type": "Property",
                            "value": "Title of linked entity 01"
                        },
                        "@context": "$NGSILD_TEST_CORE_CONTEXT"
                    },
                    {
                        "id": "urn:ngsi-ld:LinkedEntity:02",
                        "type": "LinkedEntity",
                        "@context": "$NGSILD_TEST_CORE_CONTEXT"
                    }
                ]
            """.trimIndent(),
            serializeObject(flattenedEntities)
        )
    }

    @Test
    fun `processLinkedEntities should inline an entity and apply pick on relationships`() = runTest {
        prepareMockedAnswersForJoinLevelWithPickOnEntity()

        val inlinedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    pick = parsePickOmitParameters("rel1{id,title},rel2{id,type}", null)
                        .shouldSucceedAndResult().first,
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

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
                            "title": {
                                "type": "Property",
                                "value": "Title of linked entity 01"
                            }
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
                }
            ]
            """.trimIndent(),
            serializeObject(inlinedEntities)
        )
    }

    private val linkingEntityWithTwoRelationshipsToSameEntity: CompactedEntity = """
        {
            "id": "urn:ngsi-ld:LinkingEntity:01",
            "type": "LinkingEntity",
            "rel1": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:LinkedEntity:01"
            },
            "rel2": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:LinkedEntity:01"
            }
        }
    """.trimIndent().deserializeAsMap()

    @Test
    fun `processLinkedEntities should project a linked entity along each relationship targeting it when inlined`() =
        runTest {
            prepareMockedAnswersForJoinLevelWithPickOnEntity()

            val inlinedEntities =
                linkedEntityService.processLinkedEntities(
                    linkingEntityWithTwoRelationshipsToSameEntity,
                    EntitiesQueryFromGet(
                        linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 1.toUInt()),
                        paginationQuery = PaginationQuery(0, 100),
                        pick = parsePickOmitParameters("rel1{title},rel2{description}", null)
                            .shouldSucceedAndResult().first,
                        contexts = NGSILD_TEST_CORE_CONTEXTS
                    ),
                    HttpHeaders()
                ).shouldSucceedAndResult().first

            assertJsonPayloadsAreEqual(
                """
                [{
                    "id": "urn:ngsi-ld:LinkingEntity:01",
                    "type": "LinkingEntity",
                    "rel1": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:01",
                        "entity": {
                            "title": {
                                "type": "Property",
                                "value": "Title of linked entity 01"
                            }
                        }
                    },
                    "rel2": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:LinkedEntity:01",
                        "entity": {
                            "description": {
                                "type": "Property",
                                "value": "Description of linked entity 01"
                            }
                        }
                    }
                }]
                """.trimIndent(),
                serializeObject(inlinedEntities)
            )
        }

    @Test
    fun `processLinkedEntities should merge the projections of a linked entity targeted twice when flattened`() =
        runTest {
            prepareMockedAnswersForJoinLevelWithPickOnEntity()

            val flattenedEntities =
                linkedEntityService.processLinkedEntities(
                    linkingEntityWithTwoRelationshipsToSameEntity,
                    EntitiesQueryFromGet(
                        linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 1.toUInt()),
                        paginationQuery = PaginationQuery(0, 100),
                        omit = parsePickOmitParameters(null, "rel1{title},rel2{description}")
                            .shouldSucceedAndResult().second,
                        contexts = NGSILD_TEST_CORE_CONTEXTS
                    ),
                    HttpHeaders()
                ).shouldSucceedAndResult().first

            assertJsonPayloadsAreEqual(
                """
                [
                    {
                        "id": "urn:ngsi-ld:LinkingEntity:01",
                        "type": "LinkingEntity",
                        "rel1": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:01"
                        },
                        "rel2": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:01"
                        }
                    },
                    {
                        "id": "urn:ngsi-ld:LinkedEntity:01",
                        "type": "LinkedEntity",
                        "title": {
                            "type": "Property",
                            "value": "Title of linked entity 01"
                        },
                        "description": {
                            "type": "Property",
                            "value": "Description of linked entity 01"
                        },
                        "@context": "$NGSILD_TEST_CORE_CONTEXT"
                    }
                ]
                """.trimIndent(),
                serializeObject(flattenedEntities)
            )
        }

    private fun prepareMockedAnswersForNestedProjections() {
        coEvery {
            entityQueryService.queryEntities(any())
        } coAnswers {
            Pair(
                listOf("01", "02").map {
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:$it",
                        attributes = """
                            "rel3": {
                                "type": "Relationship",
                                "object": "urn:ngsi-ld:LinkedEntity:Level2:$it"
                            }
                        """.trimIndent()
                    )
                },
                2
            ).right()
        } coAndThen {
            Pair(
                listOf("01", "02").map {
                    loadAndExpandMinimalLinkedEntity(
                        id = "urn:ngsi-ld:LinkedEntity:Level2:$it",
                        attributes = """
                            "title": {
                                "type": "Property",
                                "value": "Title of $it"
                            },
                            "description": {
                                "type": "Property",
                                "value": "Description of $it"
                            }
                        """.trimIndent()
                    )
                },
                2
            ).right()
        }
    }

    @Test
    fun `processLinkedEntities should apply nested projections according to the path of relationships`() = runTest {
        prepareMockedAnswersForNestedProjections()

        val inlinedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 2.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    pick = parsePickOmitParameters("rel1{rel3{title}},rel2{rel3{description}}", null)
                        .shouldSucceedAndResult().first,
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

        assertJsonPayloadsAreEqual(
            """
            [{
                "id": "urn:ngsi-ld:LinkingEntity:01",
                "type": "LinkingEntity",
                "rel1": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:01",
                    "entity": {
                        "rel3": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:Level2:01",
                            "entity": {
                                "title": {
                                    "type": "Property",
                                    "value": "Title of 01"
                                }
                            }
                        }
                    }
                },
                "rel2": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:LinkedEntity:02",
                    "entity": {
                        "rel3": {
                            "type": "Relationship",
                            "object": "urn:ngsi-ld:LinkedEntity:Level2:02",
                            "entity": {
                                "description": {
                                    "type": "Property",
                                    "value": "Description of 02"
                                }
                            }
                        }
                    }
                }
            }]
            """.trimIndent(),
            serializeObject(inlinedEntities)
        )
    }

    @Test
    fun `processLinkedEntities should not inline a linked entity having no member left after projection`() = runTest {
        prepareMockedAnswersForJoinLevelWithPickOnEntity()

        val inlinedEntities =
            linkedEntityService.processLinkedEntities(
                linkingEntityWithTwoRelationships,
                EntitiesQueryFromGet(
                    linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 1.toUInt()),
                    paginationQuery = PaginationQuery(0, 100),
                    pick = parsePickOmitParameters("rel1{description},rel2{description}", null)
                        .shouldSucceedAndResult().first,
                    contexts = NGSILD_TEST_CORE_CONTEXTS
                ),
                HttpHeaders()
            ).shouldSucceedAndResult().first

        val inlinedEntity = inlinedEntities.single()
        assertEquals(
            "Description of linked entity 01",
            (((inlinedEntity["rel1"] as Map<*, *>)["entity"] as Map<*, *>)["description"] as Map<*, *>)["value"]
        )
        assertFalse((inlinedEntity["rel2"] as Map<*, *>).containsKey("entity"))
    }

    private fun mockLocalLinkedEntity01() {
        coEvery {
            entityQueryService.queryEntities(any())
        } coAnswers {
            Pair(listOf(loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:01")), 1).right()
        }
    }

    @Test
    fun `processLinkedEntities should retrieve from context sources the linked entities having an objectType`() =
        runTest {
            mockLocalLinkedEntity01()
            coEvery {
                distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
            } returns Triple(emptyList(), listOf(listOf(remoteLinkedEntity) to gimmeRawCSR()), listOf(null))

            val (inlinedEntities, warnings) = linkedEntityService.processLinkedEntities(
                linkingEntityWithObjectTypes,
                inlineEntitiesQuery,
                HttpHeaders()
            ).shouldSucceedAndResult()

            assertThat(warnings).isEmpty()
            val inlinedEntity = inlinedEntities.single()
            assertThat((inlinedEntity["localRel"] as Map<*, *>)["entity"]).isNotNull
            assertEquals(remoteLinkedEntity, (inlinedEntity["remoteRel"] as Map<*, *>)["entity"])
            // not stored locally and not annotated with an objectType: not retrievable
            assertFalse((inlinedEntity["untypedRel"] as Map<*, *>).containsKey("entity"))

            coVerify {
                distributedEntityConsumptionService.distributeQueryEntitiesOperation(
                    match {
                        it.ids == setOf("urn:ngsi-ld:LinkedEntity:01".toUri(), "urn:ngsi-ld:LinkedEntity:02".toUri()) &&
                            it.typeSelection == "${NGSILD_DEFAULT_VOCAB}LinkedEntity"
                    },
                    any(),
                    match {
                        it.getFirst("id") == "urn:ngsi-ld:LinkedEntity:01,urn:ngsi-ld:LinkedEntity:02" &&
                            it.getFirst("type") == "LinkedEntity"
                    }
                )
            }
        }

    @Test
    fun `processLinkedEntities should return the warnings of context sources and keep the local linked entities`() =
        runTest {
            mockLocalLinkedEntity01()
            val warning = MiscellaneousWarning("CSR is unreachable", gimmeRawCSR())
            coEvery {
                distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
            } returns Triple(listOf(warning), emptyList(), emptyList())

            val (inlinedEntities, warnings) = linkedEntityService.processLinkedEntities(
                linkingEntityWithObjectTypes,
                inlineEntitiesQuery,
                HttpHeaders()
            ).shouldSucceedAndResult()

            assertThat(warnings).containsExactly(warning)
            val inlinedEntity = inlinedEntities.single()
            assertThat((inlinedEntity["localRel"] as Map<*, *>)["entity"]).isNotNull
            assertFalse((inlinedEntity["remoteRel"] as Map<*, *>).containsKey("entity"))
        }

    @Test
    fun `processLinkedEntities should not query context sources when the query is local`() = runTest {
        mockLocalLinkedEntity01()

        linkedEntityService.processLinkedEntities(
            linkingEntityWithObjectTypes,
            inlineEntitiesQuery.copy(local = true),
            HttpHeaders()
        ).shouldSucceed()

        coVerify(exactly = 0) {
            distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
        }
    }

    @Test
    fun `processLinkedEntities should flatten a linked entity retrieved from a context source`() = runTest {
        mockLocalLinkedEntity01()
        coEvery {
            distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
        } returns Triple(emptyList(), listOf(listOf(remoteLinkedEntity) to gimmeRawCSR()), listOf(null))

        val (flattenedEntities, warnings) = linkedEntityService.processLinkedEntities(
            linkingEntityWithObjectTypes,
            inlineEntitiesQuery.copy(linkedEntityQuery = LinkedEntityQuery(JoinType.FLAT, 1.toUInt())),
            HttpHeaders()
        ).shouldSucceedAndResult()

        assertThat(warnings).isEmpty()
        assertThat(flattenedEntities.map { it["id"] }).containsExactly(
            "urn:ngsi-ld:LinkingEntity:01",
            "urn:ngsi-ld:LinkedEntity:01",
            "urn:ngsi-ld:LinkedEntity:02"
        )
        assertEquals(remoteLinkedEntity, flattenedEntities.last())
    }

    @Test
    fun `processLinkedEntities should retrieve from context sources the linked entities of a remote linked entity`() =
        runTest {
            val remoteLinkingEntity = remoteLinkedEntity.plus(
                "remoteRel" to mapOf(
                    "type" to "Relationship",
                    "object" to "urn:ngsi-ld:LinkedEntity:04",
                    "objectType" to "LinkedEntity"
                )
            )
            val secondLevelRemoteEntity = mapOf("id" to "urn:ngsi-ld:LinkedEntity:04", "type" to "LinkedEntity")
            coEvery {
                entityQueryService.queryEntities(any())
            } coAnswers {
                val ids = (firstArg() as EntitiesQueryFromGet).ids
                if ("urn:ngsi-ld:LinkedEntity:01".toUri() in ids)
                    Pair(listOf(loadAndExpandMinimalLinkedEntity("urn:ngsi-ld:LinkedEntity:01")), 1).right()
                else Pair(emptyList<ExpandedEntity>(), 0).right()
            }
            coEvery {
                distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
            } coAnswers {
                val ids = (firstArg() as EntitiesQueryFromGet).ids
                val remoteEntity =
                    if ("urn:ngsi-ld:LinkedEntity:02".toUri() in ids) remoteLinkingEntity
                    else secondLevelRemoteEntity
                Triple(emptyList(), listOf(listOf(remoteEntity) to gimmeRawCSR()), listOf(null))
            }

            val (inlinedEntities, _) = linkedEntityService.processLinkedEntities(
                linkingEntityWithObjectTypes,
                inlineEntitiesQuery.copy(linkedEntityQuery = LinkedEntityQuery(JoinType.INLINE, 2.toUInt())),
                HttpHeaders()
            ).shouldSucceedAndResult()

            val firstLevelEntity = (inlinedEntities.single()["remoteRel"] as Map<*, *>)["entity"] as Map<*, *>
            assertEquals("urn:ngsi-ld:LinkedEntity:02", firstLevelEntity["id"])
            assertEquals(secondLevelRemoteEntity, (firstLevelEntity["remoteRel"] as Map<*, *>)["entity"])

            coVerify(exactly = 2) {
                distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
            }
        }

    @Test
    fun `processLinkedEntities should merge a linked entity retrieved from inclusive and auxiliary context sources`() =
        runTest {
            mockLocalLinkedEntity01()
            val auxiliaryLinkedEntity = remoteLinkedEntity.plus(
                listOf(
                    "name" to mapOf("type" to "Property", "value" to "auxiliary"),
                    "description" to mapOf("type" to "Property", "value" to "from auxiliary")
                )
            )
            coEvery {
                distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
            } returns Triple(
                emptyList(),
                listOf(
                    listOf(auxiliaryLinkedEntity) to
                        gimmeRawCSR(id = "urn:ngsi-ld:CSR:aux".toUri(), mode = Mode.AUXILIARY),
                    listOf(remoteLinkedEntity) to gimmeRawCSR(id = "urn:ngsi-ld:CSR:inc".toUri())
                ),
                listOf(null, null)
            )

            val (inlinedEntities, warnings) = linkedEntityService.processLinkedEntities(
                linkingEntityWithObjectTypes,
                inlineEntitiesQuery,
                HttpHeaders()
            ).shouldSucceedAndResult()

            assertThat(warnings).isEmpty()
            val mergedEntity = (inlinedEntities.single()["remoteRel"] as Map<*, *>)["entity"] as Map<*, *>
            // the inclusive CSR wins, the auxiliary CSR only fills the missing attributes
            assertEquals("remote", (mergedEntity["name"] as Map<*, *>)["value"])
            assertEquals("from auxiliary", (mergedEntity["description"] as Map<*, *>)["value"])
        }

    @Test
    fun `processLinkedEntities should merge a local linked entity with the one retrieved from a context source`() =
        runTest {
            mockLocalLinkedEntity01()
            val remoteLocalLinkedEntity = mapOf(
                "id" to "urn:ngsi-ld:LinkedEntity:01",
                "type" to "LinkedEntity",
                "description" to mapOf("type" to "Property", "value" to "from inclusive")
            )
            coEvery {
                distributedEntityConsumptionService.distributeQueryEntitiesOperation(any(), any(), any())
            } returns Triple(emptyList(), listOf(listOf(remoteLocalLinkedEntity) to gimmeRawCSR()), listOf(null))

            val (inlinedEntities, warnings) = linkedEntityService.processLinkedEntities(
                linkingEntityWithObjectTypes,
                inlineEntitiesQuery,
                HttpHeaders()
            ).shouldSucceedAndResult()

            assertThat(warnings).isEmpty()
            val mergedEntity = (inlinedEntities.single()["localRel"] as Map<*, *>)["entity"] as Map<*, *>
            assertEquals("urn:ngsi-ld:LinkedEntity:01", mergedEntity["id"])
            assertEquals("from inclusive", (mergedEntity["description"] as Map<*, *>)["value"])
        }

    private suspend fun loadAndExpandMinimalLinkedEntity(id: String, attributes: String? = null): ExpandedEntity =
        loadAndExpandMinimalEntity(id, LINKED_ENTITY_TERM, attributes)
}
