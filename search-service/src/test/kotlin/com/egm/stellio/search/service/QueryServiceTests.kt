package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
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
                    payload = EMPTY_PAYLOAD
                )
            }
        coEvery { temporalEntityAttributeService.getForEntity(any(), any()) } returns temporalEntityAttributes
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
        } returns
            listOf(
                FullAttributeInstanceResult(temporalEntityAttributes[0].id, "", null),
                FullAttributeInstanceResult(temporalEntityAttributes[1].id, "", null)
            )
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns mockkClass(EntityPayload::class).right()
        every {
            temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any(), any())
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
                listOf(APIC_COMPOUND_CONTEXT),
                withTemporalValues = false,
                withAudit = false
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
            payload = EMPTY_PAYLOAD
        )
        coEvery {
            temporalEntityAttributeService.getForEntities(any(), any())
        } returns listOf(temporalEntityAttribute)
        coEvery { temporalEntityAttributeService.getCountForEntities(any(), any()) } returns 1.right()
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
            temporalEntityService.buildTemporalEntities(any(), any(), any(), any(), any())
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
            temporalEntityAttributeService.getForEntities(
                QueryParams(
                    offset = 2,
                    limit = 2,
                    types = setOf(BEEHIVE_TYPE, APIARY_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                ),
                any()
            )
            attributeInstanceService.search(
                match { temporalQuery ->
                    temporalQuery.timerel == TemporalQuery.Timerel.BEFORE &&
                        temporalQuery.timeAt!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                },
                any<List<TemporalEntityAttribute>>(),
                false
            )
            temporalEntityAttributeService.getCountForEntities(
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
                listOf(APIC_COMPOUND_CONTEXT),
                withTemporalValues = false,
                withAudit = false
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
            payload = EMPTY_PAYLOAD
        )

        coEvery {
            temporalEntityAttributeService.getForEntities(any(), any())
        } returns listOf(temporalEntityAttribute)
        coEvery { entityPayloadService.retrieve(any<URI>()) } returns mockkClass(EntityPayload::class).right()
        coEvery { temporalEntityAttributeService.getCountForEntities(any(), any()) } returns 1.right()
        coEvery {
            attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
        } returns emptyList()
        every {
            temporalEntityService.buildTemporalEntities(any(), any(), any(), any(), any())
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
                listOf(APIC_COMPOUND_CONTEXT),
                withTemporalValues = false,
                withAudit = false
            )
        }
    }
}
