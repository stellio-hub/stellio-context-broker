package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import java.time.ZonedDateTime

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [QueryService::class])
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class QueryServiceTests {

    @Autowired
    private lateinit var queryService: QueryService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var temporalEntityService: TemporalEntityService

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should throw a 404 if the entity does not exist`() {
        every { temporalEntityAttributeService.getForEntity(any(), any()) } answers { Flux.empty() }

        val exception = assertThrows<ResourceNotFoundException> {
            runTest {
                queryService.queryTemporalEntity(
                    entityUri,
                    TemporalQuery(
                        timerel = TemporalQuery.Timerel.AFTER,
                        timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                        expandedAttrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
                    ),
                    withTemporalValues = false,
                    withAudit = false,
                    APIC_COMPOUND_CONTEXT
                )
            }
        }

        val expectedMessage =
            "Entity $entityUri does not exist or it has none of the requested attributes : " +
                "[$INCOMING_PROPERTY, $OUTGOING_PROPERTY]"
        assertEquals(expectedMessage, exception.message)
    }

    @Test
    fun `it should query a temporal entity as requested by query params`() =
        runTest {
            val temporalEntityAttributes =
                listOf("incoming", "outgoing").map {
                    TemporalEntityAttribute(
                        entityId = entityUri,
                        types = listOf(BEEHIVE_TYPE),
                        attributeName = it,
                        attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                    )
                }
            every { temporalEntityAttributeService.getForEntity(any(), any()) } answers {
                temporalEntityAttributes.toFlux()
            }
            every {
                attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
            } answers {
                Mono.just(
                    listOf(
                        FullAttributeInstanceResult(temporalEntityAttributes[0].id, "", null),
                        FullAttributeInstanceResult(temporalEntityAttributes[1].id, "", null)
                    )
                )
            }
            every {
                temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any(), any())
            } returns emptyMap()

            queryService.queryTemporalEntity(
                entityUri,
                TemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                withTemporalValues = false,
                withAudit = false,
                APIC_COMPOUND_CONTEXT
            )

            verify {
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
                    entityUri,
                    match { teaInstanceResult -> teaInstanceResult.size == 2 },
                    any(),
                    listOf(APIC_COMPOUND_CONTEXT),
                    false,
                    false
                )
            }
            confirmVerified(temporalEntityAttributeService, attributeInstanceService, temporalEntityService)
        }

    @Test
    fun `it should query temporal entities as requested by query params`() =
        runTest {
            val temporalEntityAttribute = TemporalEntityAttribute(
                entityId = entityUri,
                types = listOf(BEEHIVE_TYPE),
                attributeName = "incoming",
                attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
            )
            every {
                temporalEntityAttributeService.getForEntities(any(), any())
            } answers { Mono.just(listOf(temporalEntityAttribute)) }
            every {
                temporalEntityAttributeService.getCountForEntities(any(), any())
            } answers { Mono.just(1) }
            every {
                attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
            } answers {
                Mono.just(
                    listOf(
                        SimplifiedAttributeInstanceResult(
                            temporalEntityAttribute = temporalEntityAttribute.id,
                            value = 2.0,
                            time = ZonedDateTime.now()
                        )
                    )
                )
            }
            every {
                temporalEntityService.buildTemporalEntities(any(), any(), any(), any(), any())
            } returns emptyList()

            queryService.queryTemporalEntities(
                TemporalEntitiesQuery(
                    QueryParams(offset = 2, limit = 2, types = setOf(BEEHIVE_TYPE, APIARY_TYPE)),
                    TemporalQuery(
                        expandedAttrs = emptySet(),
                        timerel = TemporalQuery.Timerel.BEFORE,
                        timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                    ),
                    withTemporalValues = false,
                    withAudit = false
                ),
                APIC_COMPOUND_CONTEXT
            ) { null }

            verify {
                temporalEntityAttributeService.getForEntities(
                    QueryParams(offset = 2, limit = 2, types = setOf(BEEHIVE_TYPE, APIARY_TYPE)),
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
                    QueryParams(offset = 2, limit = 2, types = setOf(BEEHIVE_TYPE, APIARY_TYPE)),
                    any()
                )
                temporalEntityService.buildTemporalEntities(
                    match { it.first().first == entityUri },
                    any(),
                    listOf(APIC_COMPOUND_CONTEXT),
                    false,
                    false
                )
            }
            confirmVerified(temporalEntityAttributeService, attributeInstanceService, temporalEntityService)
        }

    @Test
    fun `it should return an empty list for a temporal entity attribute if it has no temporal values`() =
        runTest {
            val temporalEntityAttribute = TemporalEntityAttribute(
                entityId = entityUri,
                types = listOf(BEEHIVE_TYPE),
                attributeName = "incoming",
                attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
            )
            every {
                temporalEntityAttributeService.getForEntities(any(), any())
            } answers { Mono.just(listOf(temporalEntityAttribute)) }
            every {
                temporalEntityAttributeService.getCountForEntities(any(), any())
            } answers { Mono.just(1) }
            every {
                attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
            } answers { Mono.just(emptyList()) }

            every {
                temporalEntityService.buildTemporalEntities(any(), any(), any(), any(), any())
            } returns emptyList()

            val (entities, _) = queryService.queryTemporalEntities(
                TemporalEntitiesQuery(
                    QueryParams(types = setOf(BEEHIVE_TYPE, APIARY_TYPE), offset = 2, limit = 2),
                    TemporalQuery(
                        expandedAttrs = emptySet(),
                        timerel = TemporalQuery.Timerel.BEFORE,
                        timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                    ),
                    withTemporalValues = false,
                    withAudit = false
                ),
                APIC_COMPOUND_CONTEXT
            ) { null }

            assertTrue(entities.isEmpty())

            verify {
                temporalEntityService.buildTemporalEntities(
                    match {
                        it.size == 1 &&
                            it.first().first == entityUri &&
                            it.first().second.size == 1 &&
                            it.first().second.values.first().isEmpty()
                    },
                    any(),
                    listOf(APIC_COMPOUND_CONTEXT),
                    withTemporalValues = false,
                    withAudit = false
                )
            }
            confirmVerified(temporalEntityService)
        }
}
