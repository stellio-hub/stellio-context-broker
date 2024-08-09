package com.egm.stellio.search.temporal.service

import arrow.core.right
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.service.EntityAttributeService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.scope.ScopeService
import com.egm.stellio.search.support.*
import com.egm.stellio.search.temporal.model.*
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.ZonedDateTime

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [TemporalQueryService::class])
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class TemporalQueryServiceTests {

    @Autowired
    private lateinit var queryService: TemporalQueryService

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var scopeService: ScopeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var entityAttributeService: EntityAttributeService

    private val now = ngsiLdDateTime()

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should return an API exception if the entity does not exist`() = runTest {
        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns emptyList()

        queryService.queryTemporalEntity(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                entitiesQuery = EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            )
        ).fold({
            assertInstanceOf(ResourceNotFoundException::class.java, it)
            assertEquals(
                "Entity $entityUri does not exist or it has none of the requested attributes : " +
                    "[$INCOMING_PROPERTY, $OUTGOING_PROPERTY]",
                it.message
            )
        }, {
            fail("it should have returned an API exception if the entity does not exist")
        })
    }

    @Test
    fun `it should query a temporal entity as requested by query params`() = runTest {
        val attributes =
            listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY).map {
                Attribute(
                    entityId = entityUri,
                    attributeName = it,
                    attributeValueType = Attribute.AttributeValueType.NUMBER,
                    createdAt = now,
                    payload = EMPTY_JSON_PAYLOAD
                )
            }

        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns attributes
        coEvery { entityService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<Attribute>>())
        } returns
            listOf(
                FullAttributeInstanceResult(attributes[0].id, EMPTY_PAYLOAD, now, NGSILD_CREATED_AT_TERM, null),
                FullAttributeInstanceResult(attributes[1].id, EMPTY_PAYLOAD, now, NGSILD_CREATED_AT_TERM, null)
            ).right()

        queryService.queryTemporalEntity(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                entitiesQuery = EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            )
        )

        coVerify {
            entityAttributeService.getForEntity(entityUri, emptySet(), emptySet())
            attributeInstanceService.search(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.AFTER &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        )
                },
                any<List<Attribute>>()
            )
            scopeService.retrieveHistory(listOf(entityUri), any())
        }
    }

    @Test
    fun `it should not return an oldest timestamp if not in an aggregattion query`() = runTest {
        val origin = queryService.calculateOldestTimestamp(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = buildDefaultTestTemporalQuery(),
                entitiesQuery = EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            emptyList()
        )

        assertNull(origin)
    }

    @Test
    fun `it should return timeAt as the oldest timestamp if it is provided in the temporal query`() = runTest {
        val origin = queryService.calculateOldestTimestamp(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = now
                ),
                entitiesQuery = EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            emptyList()
        )

        assertNotNull(origin)
        assertEquals(now, origin)
    }

    @Test
    fun `it should return the oldest timestamp from the DB if none is provided in the temporal query`() = runTest {
        coEvery {
            attributeInstanceService.selectOldestDate(any(), any())
        } returns ZonedDateTime.parse("2023-09-03T12:34:56Z")
        coEvery {
            scopeService.selectOldestDate(any(), any())
        } returns ZonedDateTime.parse("2023-09-03T14:22:55Z")

        val origin = queryService.calculateOldestTimestamp(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = buildDefaultTestTemporalQuery(),
                entitiesQuery = EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            emptyList()
        )

        assertNotNull(origin)
        assertEquals(ZonedDateTime.parse("2023-09-03T12:34:56Z"), origin)
    }

    @Test
    fun `it should query temporal entities as requested by query params`() = runTest {
        val attribute = Attribute(
            entityId = entityUri,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        coEvery { entityService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery {
            entityAttributeService.getForTemporalEntities(any(), any())
        } returns listOf(attribute)
        coEvery { entityService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery { entityService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<Attribute>>())
        } returns
            listOf(
                SimplifiedAttributeInstanceResult(
                    attribute = attribute.id,
                    value = 2.0,
                    time = ngsiLdDateTime()
                )
            ).right()

        queryService.queryTemporalEntities(
            TemporalEntitiesQuery(
                EntitiesQuery(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                withTemporalValues = true,
                withAudit = false,
                withAggregatedValues = false
            )
        ) { null }

        coVerify {
            entityAttributeService.getForTemporalEntities(
                listOf(entityUri),
                EntitiesQuery(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            )
            attributeInstanceService.search(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.BEFORE &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        )
                },
                any<List<Attribute>>()
            )
            entityService.queryEntitiesCount(
                EntitiesQuery(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                any()
            )
            scopeService.retrieveHistory(listOf(entityUri), any())
        }
    }

    @Test
    fun `it should return an empty list for an attribute if it has no temporal values`() = runTest {
        val attribute = Attribute(
            entityId = entityUri,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        coEvery { entityService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery {
            entityAttributeService.getForTemporalEntities(any(), any())
        } returns listOf(attribute)
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<Attribute>>())
        } returns emptyList<AttributeInstanceResult>().right()
        coEvery { entityService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery { entityService.queryEntitiesCount(any(), any()) } returns 1.right()

        queryService.queryTemporalEntities(
            TemporalEntitiesQuery(
                EntitiesQuery(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                    aggrMethods = listOf(TemporalQuery.Aggregate.AVG)
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            )
        ) { null }
            .fold({
                fail("it should have returned an empty list")
            }, {
                assertThat(it.first).hasSize(1)
                assertJsonPayloadsAreEqual(
                    """
                    {
                        "@id": "urn:ngsi-ld:BeeHive:TESTC",
                        "@type": [
                            "https://ontology.eglobalmark.com/apic#BeeHive"
                        ],
                        "https://uri.etsi.org/ngsi-ld/createdAt": [
                            {
                                "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
                                "@value": "$now"
                            }
                        ],
                        "https://ontology.eglobalmark.com/apic#incoming":[
                            {
                                "@type": [
                                    "https://uri.etsi.org/ngsi-ld/Property"
                                ],
                                "https://uri.etsi.org/ngsi-ld/avg":[
                                    {
                                        "@list":[]
                                    }
                                ]
                            }
                        ]
                    }
                    """.trimIndent(),
                    JsonUtils.serializeObject(it.first[0].members),
                    setOf(NGSILD_CREATED_AT_PROPERTY)
                )
            })
    }

    private fun gimmeEntityPayload() =
        gimmeEntityPayload(
            entityId = entityUri,
            payload = loadSampleData("beehive_expanded.jsonld")
        )
}
