package com.egm.stellio.search.service

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.EMPTY_JSON_PAYLOAD
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
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
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var temporalEntityService: TemporalEntityService

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should return a JSON-LD entity when querying by id`() = runTest {
        val expandedPayload = loadSampleData("beehive_expanded.jsonld")
        val entityPayload = mockkClass(EntityPayload::class) {
            every { entityId } returns entityUri
            every { payload } returns Json.of(expandedPayload)
        }
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns entityPayload.right()

        queryService.queryEntity(entityUri, listOf(APIC_COMPOUND_CONTEXT))
            .shouldSucceedWith {
                assertEquals(entityUri.toString(), it.id)
                assertEquals(listOf(BEEHIVE_TYPE), it.types)
                assertEquals(6, it.members.size)
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
        val expandedPayload = loadSampleData("beehive_expanded.jsonld")
        val entityPayload = mockkClass(EntityPayload::class) {
            every { entityId } returns entityUri
            every { payload } returns Json.of(expandedPayload)
        }

        coEvery { entityPayloadService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery { entityPayloadService.retrieve(any<List<URI>>()) } returns listOf(entityPayload)

        queryService.queryEntities(buildDefaultQueryParams()) { null }
            .shouldSucceedWith {
                assertEquals(1, it.second)
                assertEquals(entityUri.toString(), it.first[0].id)
                assertEquals(listOf(BEEHIVE_TYPE), it.first[0].types)
                assertEquals(6, it.first[0].members.size)
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
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    limit = 0,
                    offset = 50,
                    context = APIC_COMPOUND_CONTEXT
                ),
                withTemporalValues = false,
                withAudit = false
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
        val temporalEntityAttributes =
            listOf("incoming", "outgoing").map {
                TemporalEntityAttribute(
                    entityId = entityUri,
                    attributeName = it,
                    attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
                    createdAt = now,
                    payload = EMPTY_JSON_PAYLOAD
                )
            }
        coEvery { temporalEntityAttributeService.getForEntity(any(), any()) } returns temporalEntityAttributes
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
        } returns
            listOf(
                FullAttributeInstanceResult(temporalEntityAttributes[0].id, "", now, NGSILD_CREATED_AT_TERM, null),
                FullAttributeInstanceResult(temporalEntityAttributes[1].id, "", now, NGSILD_CREATED_AT_TERM, null)
            )
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns mockkClass(EntityPayload::class).right()
        every {
            temporalEntityService.buildTemporalEntity(any(), any(), any(), any())
        } returns emptyMap()

        queryService.queryTemporalEntity(
            entityUri,
            TemporalEntitiesQuery(
                temporalQuery = TemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                queryParams = QueryParams(limit = 0, offset = 50, context = APIC_COMPOUND_CONTEXT),
                withTemporalValues = false,
                withAudit = false
            ),
            APIC_COMPOUND_CONTEXT
        )

        coVerify {
            temporalEntityAttributeService.getForEntity(entityUri, emptySet())
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.AFTER &&
                        temporalQuery.timeAt!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                any<List<TemporalEntityAttribute>>(),
                false
            )
            temporalEntityService.buildTemporalEntity(
                any(),
                match { teaInstanceResult -> teaInstanceResult.size == 2 },
                any(),
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `it should query temporal entities as requested by query params`() = runTest {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityUri,
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        coEvery { entityPayloadService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery {
            temporalEntityAttributeService.getForTemporalEntities(any(), any())
        } returns listOf(temporalEntityAttribute)
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns mockkClass(EntityPayload::class).right()
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
        } returns
            listOf(
                SimplifiedAttributeInstanceResult(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    value = 2.0,
                    time = ZonedDateTime.now()
                )
            )
        every {
            temporalEntityService.buildTemporalEntities(any(), any(), any())
        } returns emptyList()

        queryService.queryTemporalEntities(
            TemporalEntitiesQuery(
                QueryParams(
                    offset = 2,
                    limit = 2,
                    types = setOf(BEEHIVE_TYPE, APIARY_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                ),
                TemporalQuery(
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                withTemporalValues = false,
                withAudit = false
            )
        ) { null }

        coVerify {
            temporalEntityAttributeService.getForTemporalEntities(
                listOf(entityUri),
                QueryParams(
                    offset = 2,
                    limit = 2,
                    types = setOf(BEEHIVE_TYPE, APIARY_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            )
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BEFORE &&
                        temporalQuery.timeAt!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                any<List<TemporalEntityAttribute>>(),
                false
            )
            entityPayloadService.queryEntitiesCount(
                QueryParams(
                    offset = 2,
                    limit = 2,
                    types = setOf(BEEHIVE_TYPE, APIARY_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                ),
                any()
            )
            temporalEntityService.buildTemporalEntities(
                any(),
                any(),
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
    }

    @Test
    fun `it should return an empty list for a temporal entity attribute if it has no temporal values`() = runTest {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityUri,
            attributeName = "incoming",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        coEvery { entityPayloadService.queryEntities(any(), any()) } returns listOf(entityUri)
        coEvery {
            temporalEntityAttributeService.getForTemporalEntities(any(), any())
        } returns listOf(temporalEntityAttribute)
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns mockkClass(EntityPayload::class).right()
        coEvery { entityPayloadService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
        } returns emptyList()
        every {
            temporalEntityService.buildTemporalEntities(any(), any(), any())
        } returns emptyList()

        queryService.queryTemporalEntities(
            TemporalEntitiesQuery(
                QueryParams(
                    types = setOf(BEEHIVE_TYPE, APIARY_TYPE),
                    offset = 2,
                    limit = 2,
                    context = APIC_COMPOUND_CONTEXT
                ),
                TemporalQuery(
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                withTemporalValues = false,
                withAudit = false
            )
        ) { null }
            .fold({
                fail("it should have returned an empty list")
            }, {
                assertThat(it.first).isEmpty()
            })

        verify {
            temporalEntityService.buildTemporalEntities(
                match {
                    it.size == 1 &&
                        it.first().second.size == 1 &&
                        it.first().second.values.first().isEmpty()
                },
                any(),
                listOf(APIC_COMPOUND_CONTEXT)
            )
        }
    }
}
