package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.search.support.buildDefaultPagination
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.util.APIARY_TYPE
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import io.mockk.every
import io.mockk.mockkClass
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.time.ZonedDateTime

@ActiveProfiles("test")
class TemporalQueryUtilsTests {

    @Test
    fun `it should not validate the temporal query if type or attrs are not present`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        composeTemporalEntitiesQueryFromGet(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXTS,
            true
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query unless local is true",
                it.message
            )
        }
    }

    @Test
    fun `it should not validate the temporal query if timerel is present without time`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "before")

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        composeTemporalEntitiesQueryFromGet(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXTS
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("'timerel' and 'time' must be used in conjunction", it.message)
        }
    }

    @Test
    fun `it should not validate the temporal query if timerel and time are not present`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("type", "Beehive")

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        composeTemporalEntitiesQueryFromGet(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXTS,
            true
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("'timerel' and 'time' must be used in conjunction", it.message)
        }
    }

    @Test
    fun `it shouldn't validate the temporal query if both temporalValues and aggregatedValues are present`() = runTest {
        val queryParams = gimmeTemporalEntitiesQueryParams()
        queryParams.replace("options", listOf("aggregatedValues,temporalValues"))
        queryParams.add("aggrMethods", "sum")
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        composeTemporalEntitiesQueryFromGet(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXTS,
            true
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "Found different temporal representations in options query parameter, only one can be provided",
                it.message
            )
        }
    }

    @Test
    fun `it shouldn't validate the temporal query if format contains an invalid value`() = runTest {
        val queryParams = gimmeTemporalEntitiesQueryParams()
        queryParams.add("format", "invalid")
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        composeTemporalEntitiesQueryFromGet(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXTS,
            true
        ).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalid' is not a valid temporal representation", it.message)
        }
    }

    @Test
    fun `it shouldn't validate the temporal query if options contains an invalid value`() = runTest {
        val queryParams = gimmeTemporalEntitiesQueryParams()
        queryParams.replace("options", listOf("invalidOptions"))
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        composeTemporalEntitiesQueryFromGet(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXTS,
            true
        ).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalidOptions' is not a valid value for the options query parameter", it.message)
        }
    }

    @Test
    fun `it should parse a valid temporal query`() = runTest {
        val queryParams = gimmeTemporalEntitiesQueryParams()

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100
        every { pagination.temporalLimit } returns 100

        val temporalEntitiesQuery =
            composeTemporalEntitiesQueryFromGet(pagination, queryParams, APIC_COMPOUND_CONTEXTS)
                .shouldSucceedAndResult()

        assertEquals(
            setOf("urn:ngsi-ld:BeeHive:TESTC".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
            temporalEntitiesQuery.entitiesQuery.ids
        )
        assertEquals(
            "$BEEHIVE_TYPE,$APIARY_TYPE",
            temporalEntitiesQuery.entitiesQuery.typeSelection
        )
        assertEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), temporalEntitiesQuery.entitiesQuery.attrs)
        assertEquals(
            buildDefaultTestTemporalQuery(
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
            ),
            temporalEntitiesQuery.temporalQuery
        )
        assertTrue(temporalEntitiesQuery.temporalRepresentation == TemporalRepresentation.TEMPORAL_VALUES)
        assertFalse(temporalEntitiesQuery.withAudit)
        assertEquals(10, temporalEntitiesQuery.entitiesQuery.paginationQuery.limit)
        assertEquals(2, temporalEntitiesQuery.entitiesQuery.paginationQuery.offset)
        assertEquals(true, temporalEntitiesQuery.entitiesQuery.paginationQuery.count)
    }

    @Test
    fun `it should parse temporal query parameters with audit enabled`() = runTest {
        val queryParams = gimmeTemporalEntitiesQueryParams()
        queryParams["options"] = listOf("audit")

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100
        every { pagination.temporalLimit } returns 1000

        val temporalEntitiesQuery =
            composeTemporalEntitiesQueryFromGet(pagination, queryParams, APIC_COMPOUND_CONTEXTS)
                .shouldSucceedAndResult()

        assertTrue(temporalEntitiesQuery.withAudit)
    }

    private fun gimmeTemporalEntitiesQueryParams(): LinkedMultiValueMap<String, String> {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "between")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("endTimeAt", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")
        queryParams.add("id", "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTB")
        queryParams.add("datasetId", "urn:ngsi-ld:Dataset:Test1,urn:ngsi-ld:Dataset:Test2")
        queryParams.add("options", "temporalValues")
        queryParams.add("limit", "10")
        queryParams.add("offset", "2")
        queryParams.add("count", "true")
        return queryParams
    }

    @Test
    fun `it should parse a temporal query containing one attrs parameter`() = runTest {
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100
        every { pagination.temporalLimit } returns 1000

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "outgoing")

        val temporalQuery =
            composeTemporalEntitiesQueryFromGet(pagination, queryParams, APIC_COMPOUND_CONTEXTS)
                .shouldSucceedAndResult()

        assertEquals(1, temporalQuery.entitiesQuery.attrs.size)
        assertTrue(temporalQuery.entitiesQuery.attrs.contains(OUTGOING_PROPERTY))
    }

    @Test
    fun `it should parse a temporal query containing two attrs parameter`() = runTest {
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100
        every { pagination.temporalLimit } returns 1000

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "incoming,outgoing")

        val temporalQuery =
            composeTemporalEntitiesQueryFromGet(pagination, queryParams, APIC_COMPOUND_CONTEXTS)
                .shouldSucceedAndResult()

        assertEquals(2, temporalQuery.entitiesQuery.attrs.size)
        assertIterableEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), temporalQuery.entitiesQuery.attrs)
    }

    @Test
    fun `it should parse a temporal query containing no attrs parameter`() = runTest {
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100
        every { pagination.temporalLimit } returns 1000

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")

        val temporalQuery =
            composeTemporalEntitiesQueryFromGet(pagination, queryParams, APIC_COMPOUND_CONTEXTS)
                .shouldSucceedAndResult()
        assertTrue(temporalQuery.entitiesQuery.attrs.isEmpty())
    }

    @Test
    fun `it should parse lastN parameter if it is a positive integer`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "2")

        val temporalQuery = buildTemporalQuery(
            queryParams,
            buildDefaultPagination(),
            false,
            TemporalRepresentation.NORMALIZED
        ).shouldSucceedAndResult()

        assertEquals(2, temporalQuery.instanceLimit)
        assertEquals(2, temporalQuery.lastN)
    }

    @Test
    fun `it should ignore lastN parameter if it is not an integer`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "A")
        val pagination = buildDefaultPagination()
        val temporalQuery = buildTemporalQuery(
            queryParams,
            pagination,
            false,
            TemporalRepresentation.NORMALIZED
        ).shouldSucceedAndResult()

        assertEquals(pagination.temporalLimit, temporalQuery.instanceLimit)
        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should ignore lastN parameter if it is not a positive integer`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "-2")
        val pagination = buildDefaultPagination()

        val temporalQuery = buildTemporalQuery(
            queryParams,
            pagination,
            false,
            TemporalRepresentation.NORMALIZED
        ).shouldSucceedAndResult()

        assertEquals(pagination.temporalLimit, temporalQuery.instanceLimit)
        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should treat time and timerel properties as optional in a temporal query`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()

        val temporalQuery = buildTemporalQuery(
            queryParams,
            buildDefaultPagination(),
            false,
            TemporalRepresentation.NORMALIZED
        ).shouldSucceedAndResult()

        assertNull(temporalQuery.timeAt)
        assertNull(temporalQuery.timerel)
    }

    @Test
    fun `it should parse a temporal query containing a timeproperty parameter`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timeproperty", "createdAt")

        val temporalQuery = buildTemporalQuery(
            queryParams,
            buildDefaultPagination(),
            false,
            TemporalRepresentation.NORMALIZED
        ).shouldSucceedAndResult()

        assertEquals(AttributeInstance.TemporalProperty.CREATED_AT, temporalQuery.timeproperty)
    }

    @Test
    fun `it should set timeproperty to observedAt if no value is provided in query parameters`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()

        val temporalQuery = buildTemporalQuery(
            queryParams,
            buildDefaultPagination(),
            false,
            TemporalRepresentation.NORMALIZED
        ).shouldSucceedAndResult()

        assertEquals(AttributeInstance.TemporalProperty.OBSERVED_AT, temporalQuery.timeproperty)
    }

    @Test
    fun `it should parse a temporal query containing one datasetId parameter`() = runTest {
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100
        every { pagination.temporalLimit } returns 1000

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2024-11-07T07:31:39Z")
        queryParams.add("datasetId", "urn:ngsi-ld:Dataset:Test1")

        val temporalQuery =
            composeTemporalEntitiesQueryFromGet(pagination, queryParams, APIC_COMPOUND_CONTEXTS)
                .shouldSucceedAndResult()
        assertEquals(1, temporalQuery.entitiesQuery.datasetId.size)
        assertEquals("urn:ngsi-ld:Dataset:Test1", temporalQuery.entitiesQuery.datasetId.first())
    }

    @Test
    fun `it should parse a temporal query containing two datasetIds parameter`() = runTest {
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100
        every { pagination.temporalLimit } returns 1000

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2024-11-07T07:31:39Z")
        queryParams.add("datasetId", "urn:ngsi-ld:Dataset:Test1,urn:ngsi-ld:Dataset:Test2")

        val temporalQuery =
            composeTemporalEntitiesQueryFromGet(pagination, queryParams, APIC_COMPOUND_CONTEXTS)
                .shouldSucceedAndResult()
        assertEquals(2, temporalQuery.entitiesQuery.datasetId.size)
        assertIterableEquals(
            setOf("urn:ngsi-ld:Dataset:Test1", "urn:ngsi-ld:Dataset:Test2"),
            temporalQuery.entitiesQuery.datasetId
        )
    }

    @Test
    fun `it should parse a Query datatype with a TemporalQuery`() = runTest {
        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "BeeHive"
                }],
                "attrs": ["attr1"],
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2024-11-07T07:31:39Z",
                    "endTimeAt": "2024-11-12T07:31:39Z",
                    "timeproperty": "modifiedAt"
                }
            }
        """.trimIndent()

        composeTemporalEntitiesQueryFromPost(
            buildDefaultPagination(30, 100),
            Query(query).shouldSucceedAndResult(),
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedWith {
            assertThat((it.entitiesQuery as EntitiesQueryFromPost).entitySelectors)
                .hasSize(1)
                .contains(EntitySelector(id = null, idPattern = null, typeSelection = BEEHIVE_TYPE))
            assertThat(it.temporalQuery)
                .isEqualTo(
                    TemporalQuery(
                        timerel = TemporalQuery.Timerel.BETWEEN,
                        timeAt = ZonedDateTime.parse("2024-11-07T07:31:39Z"),
                        endTimeAt = ZonedDateTime.parse("2024-11-12T07:31:39Z"),
                        timeproperty = AttributeInstance.TemporalProperty.MODIFIED_AT,
                        instanceLimit = 100
                    )
                )
        }
    }
}
