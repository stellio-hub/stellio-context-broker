package com.egm.stellio.search.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.scope.ScopeService
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.EMPTY_PAYLOAD
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [QueryService::class])
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class QueryServiceTests {

    @Autowired
    private lateinit var queryService: QueryService

    @MockkBean
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var scopeService: ScopeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should return a JSON-LD entity when querying by id`() = runTest {
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()

        queryService.queryEntity(entityUri, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith {
                assertEquals(entityUri.toString(), it.id)
                assertEquals(listOf(BEEHIVE_TYPE), it.types)
                assertEquals(7, it.members.size)
            }
    }

    @Test
    fun `it should return an API exception if no entity exists with the given id`() = runTest {
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns ResourceNotFoundException("").left()

        queryService.queryEntity(entityUri, listOf(APIC_COMPOUND_CONTEXT))
            .shouldFail {
                assertTrue(it is ResourceNotFoundException)
            }
    }

    @Test
    fun `it should return a list of JSON-LD entities when querying entities`() = runTest {
        coEvery { entityPayloadService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery { entityPayloadService.retrieve(any<List<URI>>()) } returns listOf(gimmeEntityPayload())

        queryService.queryEntities(buildDefaultQueryParams()) { null }
            .shouldSucceedWith {
                assertEquals(1, it.second)
                assertEquals(entityUri.toString(), it.first[0].id)
                assertEquals(listOf(BEEHIVE_TYPE), it.first[0].types)
                assertEquals(7, it.first[0].members.size)
            }
    }

    @Test
    fun `it should return an empty list if no entity matched the query`() = runTest {
        coEvery { entityPayloadService.queryEntities(any(), any()) } returns emptyList()
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 0.right()

        queryService.queryEntities(buildDefaultQueryParams()) { null }
            .shouldSucceedWith {
                assertEquals(0, it.second)
                assertTrue(it.first.isEmpty())
            }
    }

    @Test
    fun `it should return an API exception if the entity does not exist`() = runTest {
        coEvery { temporalEntityAttributeService.getForEntity(any(), any()) } returns emptyList()

        queryService.queryTemporalEntity(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = TemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                queryParams = QueryParams(
                    limit = 0,
                    offset = 50,
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            APIC_COMPOUND_CONTEXT
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
        val teas =
            listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY).map {
                TemporalEntityAttribute(
                    entityId = entityUri,
                    attributeName = it,
                    attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
                    createdAt = now,
                    payload = EMPTY_JSON_PAYLOAD
                )
            }

        coEvery { temporalEntityAttributeService.getForEntity(any(), any()) } returns teas
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>())
        } returns
            listOf(
                FullAttributeInstanceResult(teas[0].id, EMPTY_PAYLOAD, now, NGSILD_CREATED_AT_TERM, null),
                FullAttributeInstanceResult(teas[1].id, EMPTY_PAYLOAD, now, NGSILD_CREATED_AT_TERM, null)
            ).right()

        queryService.queryTemporalEntity(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = TemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                queryParams = QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            APIC_COMPOUND_CONTEXT
        )

        coVerify {
            temporalEntityAttributeService.getForEntity(entityUri, emptySet())
            attributeInstanceService.search(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.AFTER &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        )
                },
                any<List<TemporalEntityAttribute>>()
            )
            scopeService.retrieveHistory(listOf(entityUri), any())
        }
    }

    @Test
    fun `it should not return an oldest timestamp if not in an aggregattion query`() = runTest {
        val origin = queryService.calculateOldestTimestamp(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = TemporalQuery(),
                queryParams = QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT),
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
                temporalQuery = TemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = now
                ),
                queryParams = QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT),
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
                temporalQuery = TemporalQuery(),
                queryParams = QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT),
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
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityUri,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        coEvery { entityPayloadService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery {
            temporalEntityAttributeService.getForTemporalEntities(any(), any())
        } returns listOf(temporalEntityAttribute)
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>())
        } returns
            listOf(
                SimplifiedAttributeInstanceResult(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    value = 2.0,
                    time = ngsiLdDateTime()
                )
            ).right()

        queryService.queryTemporalEntities(
            TemporalEntitiesQuery(
                QueryParams(
                    type = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    limit = 2,
                    offset = 2,
                    context = APIC_COMPOUND_CONTEXT
                ),
                TemporalQuery(
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                withTemporalValues = true,
                withAudit = false,
                withAggregatedValues = false
            )
        ) { null }

        coVerify {
            temporalEntityAttributeService.getForTemporalEntities(
                listOf(entityUri),
                QueryParams(
                    type = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    limit = 2,
                    offset = 2,
                    context = APIC_COMPOUND_CONTEXT
                )
            )
            attributeInstanceService.search(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.BEFORE &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        )
                },
                any<List<TemporalEntityAttribute>>()
            )
            entityPayloadService.queryEntitiesCount(
                QueryParams(
                    type = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    limit = 2,
                    offset = 2,
                    context = APIC_COMPOUND_CONTEXT
                ),
                any()
            )
            scopeService.retrieveHistory(listOf(entityUri), any())
        }
    }

    @Test
    fun `it should return an empty list for an attribute if it has no temporal values`() = runTest {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityUri,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        coEvery { entityPayloadService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery {
            temporalEntityAttributeService.getForTemporalEntities(any(), any())
        } returns listOf(temporalEntityAttribute)
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>())
        } returns emptyList<AttributeInstanceResult>().right()
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 1.right()

        queryService.queryTemporalEntities(
            TemporalEntitiesQuery(
                QueryParams(
                    type = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    limit = 2,
                    offset = 2,
                    context = APIC_COMPOUND_CONTEXT
                ),
                TemporalQuery(
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
                        "id": "urn:ngsi-ld:BeeHive:TESTC",
                        "type": "BeeHive",
                        "incoming": {
                            "type": "Property",
                            "avg": []
                        }
                    }
                    """.trimIndent(),
                    JsonUtils.serializeObject(it.first[0])
                )
            })
    }

    private fun gimmeEntityPayload(): EntityPayload =
        EntityPayload(
            entityId = entityUri,
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            contexts = listOf(APIC_COMPOUND_CONTEXT),
            payload = Json.of(loadSampleData("beehive_expanded.jsonld"))
        )
}
