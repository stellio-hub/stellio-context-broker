package com.egm.stellio.search.service

import com.egm.stellio.search.config.CoroutineTestRule
import com.egm.stellio.search.model.FullAttributeInstanceResult
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Rule
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import java.time.ZonedDateTime

@SpringBootTest(classes = [QueryService::class])
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class QueryServiceTests {

    @get:Rule
    var coroutinesTestRule = CoroutineTestRule()

    @Autowired
    private lateinit var queryService: QueryService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var temporalEntityService: TemporalEntityService

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    private val secondEntityUri = "urn:ngsi-ld:BeeHive:TESTB".toUri()

    private val beehiveType = "https://ontology.eglobalmark.com/apic#BeeHive"
    private val apiaryType = "https://ontology.eglobalmark.com/apic#Apiary"
    private val incomingAttrExpandedName = "https://ontology.eglobalmark.com/apic#incoming"
    private val outgoingAttrExpandedName = "https://ontology.eglobalmark.com/apic#outgoing"

    @Test
    fun `it should throw a BadRequestData exception if timerel is present without time`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "before")

        val exception = assertThrows<BadRequestDataException> {
            queryService.parseAndCheckQueryParams(queryParams, APIC_COMPOUND_CONTEXT)
        }
        assertEquals(
            "'timerel' and 'time' must be used in conjunction",
            exception.message
        )
    }

    @Test
    fun `it should throw a BadRequestData exception if neither type nor attrs is present`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "before")
        queryParams.add("time", "2019-10-17T07:31:39Z")

        val exception = assertThrows<BadRequestDataException> {
            queryService.parseAndCheckQueryParams(queryParams, APIC_COMPOUND_CONTEXT)
        }
        assertEquals(
            "Either type or attrs need to be present in request parameters",
            exception.message
        )
    }

    @Test
    fun `it should parse query parameters`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "between")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("endTime", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")
        queryParams.add("id", "$entityUri,$secondEntityUri")
        queryParams.add("options", "temporalValues")

        val parsedParams = queryService.parseAndCheckQueryParams(queryParams, APIC_COMPOUND_CONTEXT)

        assertEquals(
            parsedParams,
            mapOf(
                "ids" to setOf(entityUri, secondEntityUri),
                "types" to setOf(beehiveType, apiaryType),
                "temporalQuery" to TemporalQuery(
                    timerel = TemporalQuery.Timerel.BETWEEN,
                    time = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                    endTime = ZonedDateTime.parse("2019-10-18T07:31:39Z"),
                    expandedAttrs = setOf(incomingAttrExpandedName, outgoingAttrExpandedName)
                ),
                "withTemporalValues" to true
            )
        )
    }

    @Test
    fun `it should throw a 404 if the entity does not exist`() {
        every { temporalEntityAttributeService.getForEntity(any(), any()) } answers {
            Flux.empty()
        }

        val exception = assertThrows<ResourceNotFoundException> {
            coroutinesTestRule.testDispatcher.runBlockingTest {
                queryService.queryTemporalEntity(
                    entityUri,
                    TemporalQuery(
                        timerel = TemporalQuery.Timerel.AFTER,
                        time = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                        expandedAttrs = setOf(incomingAttrExpandedName, outgoingAttrExpandedName)
                    ),
                    false,
                    APIC_COMPOUND_CONTEXT
                )
            }
        }

        val expectedMessage =
            "Entity $entityUri does not exist or it has none of the requested attributes : " +
                "[$incomingAttrExpandedName, $outgoingAttrExpandedName]"
        assertEquals(expectedMessage, exception.message)
    }

    @Test
    fun `it should query a temporal entity as requested by query params`() =
        coroutinesTestRule.testDispatcher.runBlockingTest {
            val temporalEntityAttributes =
                listOf("incoming", "outgoing").map {
                    TemporalEntityAttribute(
                        entityId = entityUri,
                        type = "BeeHive",
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
                        FullAttributeInstanceResult(temporalEntityAttributes[0].id, ""),
                        FullAttributeInstanceResult(temporalEntityAttributes[1].id, "")
                    )
                )
            }
            every { temporalEntityService.buildTemporalEntity(any(), any(), any(), any(), any()) } returns emptyMap()

            queryService.queryTemporalEntity(
                entityUri,
                TemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    time = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                false,
                APIC_COMPOUND_CONTEXT
            )

            io.mockk.verify {
                temporalEntityAttributeService.getForEntity(entityUri, emptySet(), false)
            }

            io.mockk.verify {
                attributeInstanceService.search(
                    match { temporalQuery ->
                        temporalQuery.timerel == TemporalQuery.Timerel.AFTER &&
                            temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                    },
                    any<List<TemporalEntityAttribute>>(),
                    false
                )
            }

            io.mockk.verify {
                temporalEntityService.buildTemporalEntity(
                    entityUri,
                    match { teaInstanceResult -> teaInstanceResult.size == 2 },
                    any(),
                    listOf(APIC_COMPOUND_CONTEXT),
                    false
                )
            }
        }

    @Test
    fun `it should query temporal entities as requested by query params`() =
        coroutinesTestRule.testDispatcher.runBlockingTest {
            val temporalEntityAttribute = TemporalEntityAttribute(
                entityId = entityUri,
                type = "BeeHive",
                attributeName = "incoming",
                attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
            )
            every { temporalEntityAttributeService.getForEntities(any(), any(), any()) } returns Mono.just(
                listOf(temporalEntityAttribute)
            )
            every {
                attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
            } returns Mono.just(
                listOf(
                    SimplifiedAttributeInstanceResult(
                        temporalEntityAttribute = temporalEntityAttribute.id,
                        value = 2.0,
                        observedAt = ZonedDateTime.now()
                    )
                )
            )
            every { temporalEntityService.buildTemporalEntities(any(), any(), any(), any()) } returns emptyList()

            queryService.queryTemporalEntities(
                emptySet(),
                setOf(beehiveType, apiaryType),
                TemporalQuery(
                    expandedAttrs = emptySet(),
                    timerel = TemporalQuery.Timerel.BEFORE,
                    time = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                false,
                APIC_COMPOUND_CONTEXT
            )

            io.mockk.verify {
                temporalEntityAttributeService.getForEntities(
                    emptySet(),
                    setOf(beehiveType, apiaryType),
                    emptySet()
                )
            }

            io.mockk.verify {
                attributeInstanceService.search(
                    match { temporalQuery ->
                        temporalQuery.timerel == TemporalQuery.Timerel.BEFORE &&
                            temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                    },
                    any<List<TemporalEntityAttribute>>(),
                    false
                )
            }

            io.mockk.verify {
                temporalEntityService.buildTemporalEntities(
                    match { it.first().first == entityUri },
                    any(),
                    listOf(APIC_COMPOUND_CONTEXT),
                    false
                )
            }
        }

    @Test
    fun `it should return an empty list for a temporal entity attribute if it has no temporal values`() =
        coroutinesTestRule.testDispatcher.runBlockingTest {
            val temporalEntityAttribute = TemporalEntityAttribute(
                entityId = entityUri,
                type = "BeeHive",
                attributeName = "incoming",
                attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
            )
            every { temporalEntityAttributeService.getForEntities(any(), any(), any()) } returns Mono.just(
                listOf(temporalEntityAttribute)
            )
            every {
                attributeInstanceService.search(any(), any<List<TemporalEntityAttribute>>(), any())
            } returns Mono.just(emptyList())

            every { temporalEntityService.buildTemporalEntities(any(), any(), any(), any()) } returns emptyList()

            val entitiesList = queryService.queryTemporalEntities(
                emptySet(),
                setOf(beehiveType, apiaryType),
                TemporalQuery(
                    expandedAttrs = emptySet(),
                    timerel = TemporalQuery.Timerel.BEFORE,
                    time = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                false,
                APIC_COMPOUND_CONTEXT
            )

            assertTrue(entitiesList.isEmpty())

            io.mockk.verify {
                temporalEntityService.buildTemporalEntities(
                    match {
                        it.size == 1 &&
                            it.first().first == entityUri &&
                            it.first().second.size == 1 &&
                            it.first().second.values.first().isEmpty()
                    },
                    any(),
                    listOf(APIC_COMPOUND_CONTEXT),
                    false
                )
            }
        }
}
