package com.egm.stellio.search.util

import com.egm.stellio.search.config.ApplicationProperties
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.*
import io.mockk.every
import io.mockk.mockkClass
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.time.ZonedDateTime

@ActiveProfiles("test")
class QueryUtilsTests {

    @Test
    fun `it should throw a BadRequestData exception if timerel is present without time`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "before")

        val exception = assertThrows<BadRequestDataException> {
            parseAndCheckQueryParams(
                mockkClass(ApplicationProperties.Pagination::class),
                queryParams,
                APIC_COMPOUND_CONTEXT
            )
        }
        assertEquals(
            "'timerel' and 'time' must be used in conjunction",
            exception.message
        )
    }

    @Test
    fun `it should parse query parameters`() {
        val queryParams = gimmeFullParamsMap()

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        val temporalEntitiesQuery = parseAndCheckQueryParams(pagination, queryParams, APIC_COMPOUND_CONTEXT)

        assertEquals(
            setOf("urn:ngsi-ld:BeeHive:TESTC".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
            temporalEntitiesQuery.queryParams.ids
        )
        assertEquals(setOf(BEEHIVE_TYPE, APIARY_TYPE), temporalEntitiesQuery.queryParams.types)
        assertEquals(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z"),
                expandedAttrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY)
            ),
            temporalEntitiesQuery.temporalQuery
        )
        assertTrue(temporalEntitiesQuery.withTemporalValues)
        assertFalse(temporalEntitiesQuery.withAudit)
        assertEquals(10, temporalEntitiesQuery.queryParams.limit)
        assertEquals(2, temporalEntitiesQuery.queryParams.offset)
        assertEquals(true, temporalEntitiesQuery.queryParams.count)
    }

    @Test
    fun `it should parse query parameters with audit enabled`() {
        val queryParams = gimmeFullParamsMap()
        queryParams["options"] = listOf("audit")

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        val temporalEntitiesQuery = parseAndCheckQueryParams(pagination, queryParams, APIC_COMPOUND_CONTEXT)

        assertTrue(temporalEntitiesQuery.withAudit)
    }

    private fun gimmeFullParamsMap(): LinkedMultiValueMap<String, String> {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "between")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("endTimeAt", "2019-10-18T07:31:39Z")
        queryParams.add("type", "BeeHive,Apiary")
        queryParams.add("attrs", "incoming,outgoing")
        queryParams.add("id", "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTB")
        queryParams.add("options", "temporalValues")
        queryParams.add("limit", "10")
        queryParams.add("offset", "2")
        queryParams.add("count", "true")
        return queryParams
    }

    @Test
    fun `it should parse a query containing one attrs parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "outgoing")

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertEquals(1, temporalQuery.expandedAttrs.size)
        assertTrue(temporalQuery.expandedAttrs.contains(OUTGOING_PROPERTY))
    }

    @Test
    fun `it should parse a query containing two attrs parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "incoming,outgoing")

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertEquals(2, temporalQuery.expandedAttrs.size)
        assertIterableEquals(listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), temporalQuery.expandedAttrs)
    }

    @Test
    fun `it should parse a query containing no attrs parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertTrue(temporalQuery.expandedAttrs.isEmpty())
    }

    @Test
    fun `it should parse lastN parameter if it is a positive integer`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "2")

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertEquals(2, temporalQuery.lastN)
    }

    @Test
    fun `it should ignore lastN parameter if it is not an integer`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "A")

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should ignore lastN parameter if it is not a positive integer`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "-2")

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should treat time and timerel properties as optional`() {
        val queryParams = LinkedMultiValueMap<String, String>()

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertNull(temporalQuery.timeAt)
        assertNull(temporalQuery.timerel)
    }

    @Test
    fun `it should parse a query containing a timeproperty parameter`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timeproperty", "createdAt")

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertEquals(AttributeInstance.TemporalProperty.CREATED_AT, temporalQuery.timeproperty)
    }

    @Test
    fun `it should set timeproperty to observedAt if no value is provided in query parameters`() {
        val queryParams = LinkedMultiValueMap<String, String>()

        val temporalQuery = buildTemporalQuery(queryParams, APIC_COMPOUND_CONTEXT)

        assertEquals(AttributeInstance.TemporalProperty.OBSERVED_AT, temporalQuery.timeproperty)
    }
}
