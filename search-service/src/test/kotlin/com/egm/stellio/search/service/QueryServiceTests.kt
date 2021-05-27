package com.egm.stellio.search.service

import com.egm.stellio.search.config.CoroutineTestRule
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.toUri
import com.github.tomakehurst.wiremock.client.WireMock.get
import com.github.tomakehurst.wiremock.client.WireMock.verify
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Rule
import org.junit.jupiter.api.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import reactor.core.publisher.Mono
import java.net.URI
import java.time.ZonedDateTime

@SpringBootTest
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

    @Test
    fun `it should throw a BadRequestData exception if timerel is present without time`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "before")

        val exception = assertThrows<BadRequestDataException> {
            queryService.parseAndCheckQueryParams(queryParams, APIC_COMPOUND_CONTEXT)
        }
        Assertions.assertEquals(
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
        Assertions.assertEquals(
            "Either type or attrs need to be present in request parameters",
            exception.message
        )
    }

    @Test
    fun `it should parse query parameters`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "before")
        queryParams.add("time", "2019-10-17T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")

        val parsedParams = queryService.parseAndCheckQueryParams(queryParams, APIC_COMPOUND_CONTEXT)

        Assertions.assertEquals(
            parsedParams,
            mapOf(
                "ids" to emptySet<URI>(),
                "types" to setOf(
                    "https://ontology.eglobalmark.com/apic#BeeHive",
                    "https://ontology.eglobalmark.com/apic#Apiary"
                ),
                "temporalQuery" to TemporalQuery(
                    expandedAttrs = emptySet(),
                    timerel = TemporalQuery.Timerel.BEFORE,
                    time = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                "withTemporalValues" to false
            )
        )
    }

    @Test
    fun `it should query temporal entities as requested by query params`() =
        coroutinesTestRule.testDispatcher.runBlockingTest {
            val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
            every { temporalEntityAttributeService.getForEntities(any(), any(), any()) } returns Mono.just(
                mapOf(
                    entityId to listOf(
                        TemporalEntityAttribute(
                            entityId = entityId,
                            type = "BeeHive",
                            attributeName = "incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        )
                    )
                )
            )
            every { attributeInstanceService.search(any(), any(), any()) } returns Mono.just(emptyList())
            every { temporalEntityService.buildTemporalEntities(any(), any(), any(), any()) } returns emptyList()

            queryService.queryTemporalEntities(
                emptySet(),
                setOf(
                    "https://ontology.eglobalmark.com/apic#BeeHive",
                    "https://ontology.eglobalmark.com/apic#Apiary"
                ),
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
                    setOf(
                        "https://ontology.eglobalmark.com/apic#BeeHive",
                        "https://ontology.eglobalmark.com/apic#Apiary"
                    ),
                    emptySet()
                )
            }

            io.mockk.verify {
                attributeInstanceService.search(
                    match { temporalQuery ->
                        temporalQuery.timerel == TemporalQuery.Timerel.BEFORE &&
                            temporalQuery.time!!.isEqual(ZonedDateTime.parse("2019-10-17T07:31:39Z"))
                    },
                    any(),
                    false
                )
            }

            io.mockk.verify {
                temporalEntityService.buildTemporalEntities(
                    match { it.first().first == entityId },
                    any(),
                    listOf(APIC_COMPOUND_CONTEXT),
                    false
                )
            }
        }
}
