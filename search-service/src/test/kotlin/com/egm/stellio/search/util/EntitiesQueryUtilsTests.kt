package com.egm.stellio.search.util

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVATION_SPACE_PROPERTY
import io.mockk.every
import io.mockk.mockkClass
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import java.net.URI
import java.time.ZonedDateTime

@OptIn(ExperimentalCoroutinesApi::class)
@ActiveProfiles("test")
class EntitiesQueryUtilsTests {

    @Test
    fun `it should parse query parameters`() = runTest {
        val requestParams = gimmeEntitiesQueryParams()
        val entitiesQuery = parseQueryParams(Pair(1, 20), requestParams, APIC_COMPOUND_CONTEXT).shouldSucceedAndResult()

        assertEquals("$BEEHIVE_TYPE,$APIARY_TYPE", entitiesQuery.type)
        assertEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), entitiesQuery.attrs)
        assertEquals(
            setOf("urn:ngsi-ld:BeeHive:TESTC".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
            entitiesQuery.ids
        )
        assertEquals(".*BeeHive.*", entitiesQuery.idPattern)
        assertEquals("brandName!=Mercedes", entitiesQuery.q)
        assertEquals(true, entitiesQuery.paginationQuery.count)
        assertEquals(1, entitiesQuery.paginationQuery.offset)
        assertEquals(10, entitiesQuery.paginationQuery.limit)
        assertEquals(true, entitiesQuery.useSimplifiedRepresentation)
        assertEquals(false, entitiesQuery.includeSysAttrs)
    }

    @Test
    fun `it should set includeSysAttrs at true if options contains includeSysAttrs query parameters`() = runTest {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("options", "sysAttrs")
        val queryParams = parseQueryParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT).shouldSucceedAndResult()

        assertEquals(true, queryParams.includeSysAttrs)
    }

    @Test
    fun `it should decode q in query parameters`() = runTest {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("q", "speed%3E50%3BfoodName%3D%3Ddietary+fibres")
        val entitiesQuery = parseQueryParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT).shouldSucceedAndResult()

        assertEquals("speed>50;foodName==dietary fibres", entitiesQuery.q)
    }

    @Test
    fun `it should set default values in query parameters`() = runTest {
        val requestParams = LinkedMultiValueMap<String, String>()
        val entitiesQuery = parseQueryParams(Pair(30, 100), requestParams, NGSILD_CORE_CONTEXT).shouldSucceedAndResult()

        assertEquals(null, entitiesQuery.type)
        assertEquals(emptySet<String>(), entitiesQuery.attrs)
        assertEquals(emptySet<URI>(), entitiesQuery.ids)
        assertEquals(null, entitiesQuery.idPattern)
        assertEquals(null, entitiesQuery.q)
        assertEquals(false, entitiesQuery.paginationQuery.count)
        assertEquals(0, entitiesQuery.paginationQuery.offset)
        assertEquals(30, entitiesQuery.paginationQuery.limit)
        assertEquals(false, entitiesQuery.useSimplifiedRepresentation)
        assertEquals(false, entitiesQuery.includeSysAttrs)
    }

    private fun gimmeEntitiesQueryParams(): LinkedMultiValueMap<String, String> {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("type", "BeeHive,Apiary")
        requestParams.add("attrs", "incoming,outgoing")
        requestParams.add("id", "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTB")
        requestParams.add("idPattern", ".*BeeHive.*")
        requestParams.add("q", "brandName!=Mercedes")
        requestParams.add("count", "true")
        requestParams.add("offset", "1")
        requestParams.add("limit", "10")
        requestParams.add("options", "keyValues")
        return requestParams
    }

    @Test
    fun `it should parse a valid complete query`() = runTest {
        val query = """
            {
                "type": "Query",
                "entities": [{
                    "id": "urn:ngsi-ld:BeeHive:TESTC",
                    "idPattern": "urn:ngsi-ld:BeeHive:*",
                    "type": "BeeHive"
                }],
                "attrs": ["attr1", "attr2"],
                "q": "temperature>32",
                "geoQ": {
                    "geometry": "Point",
                    "coordinates": [1.0, 1.0],
                    "georel": "equals",
                    "geoproperty": "observationSpace"
                },
                "temporalQ": {
                    "timerel": "between",
                    "timeAt": "2023-10-01T12:34:56Z",
                    "endTimeAt": "2023-10-02T12:34:56Z",
                    "lastN": 10,
                    "timeproperty": "observedAt"
                },
                "scopeQ": "/Nantes"
            }
        """.trimIndent()

        parseQueryParamsForPost(
            Pair(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXT
        ).shouldSucceedWith {
            assertEquals(setOf("urn:ngsi-ld:BeeHive:TESTC".toUri()), it.ids)
            assertEquals("urn:ngsi-ld:BeeHive:*", it.idPattern)
            assertEquals(BEEHIVE_TYPE, it.type)
            assertEquals(setOf("${NGSILD_DEFAULT_VOCAB}attr1", "${NGSILD_DEFAULT_VOCAB}attr2"), it.attrs)
            assertEquals("temperature>32", it.q)
            assertEquals(GeoQuery.GeometryType.POINT, it.geoQuery?.geometry)
            assertEquals("[1.0, 1.0]", it.geoQuery?.coordinates)
            assertEquals(GEO_QUERY_GEOREL_EQUALS, it.geoQuery?.georel)
            assertEquals(NGSILD_OBSERVATION_SPACE_PROPERTY, it.geoQuery?.geoproperty)
            assertEquals("/Nantes", it.scopeQ)
        }
    }

    @Test
    fun `it should parse a valid simple query`() = runTest {
        val query = """
            {
                "type": "Query",
                "entities": [{
                    "type": "BeeHive"
                }],
                "attrs": ["attr1"],
                "q": "temperature>32"
            }
        """.trimIndent()

        parseQueryParamsForPost(
            Pair(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXT
        ).shouldSucceedWith {
            assertEquals(BEEHIVE_TYPE, it.type)
            assertEquals(setOf("${NGSILD_DEFAULT_VOCAB}attr1"), it.attrs)
            assertEquals("temperature>32", it.q)
        }
    }

    @Test
    fun `it should not validate a query if the type is not correct`() {
        val query = """
            {
                "type": "NotAQuery",
                "attrs": ["attr1", "attr2"]
            }
        """.trimIndent()

        parseQueryParamsForPost(
            Pair(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXT
        ).shouldFailWith {
            it is BadRequestDataException &&
                it.message == "The type parameter should be equals to 'Query'"
        }
    }

    @Test
    fun `it should not validate a query if the payload could not be parsed because the JSON is invalid`() {
        val query = """
            {
                "type": "Query",,
                "attrs": ["attr1", "attr2"]
            }
        """.trimIndent()

        parseQueryParamsForPost(
            Pair(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXT
        ).shouldFailWith {
            it is BadRequestDataException &&
                it.message.startsWith("The supplied query could not be parsed")
        }
    }

    @Test
    fun `it should not validate a query if the payload contains unexpected parameters`() {
        val query = """
            {
                "type": "Query",
                "property": "anUnexpectedProperty"
            }
        """.trimIndent()

        parseQueryParamsForPost(
            Pair(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXT
        ).shouldFailWith {
            it is BadRequestDataException &&
                it.message.startsWith("The supplied query could not be parsed")
        }
    }

    @Test
    fun `it should not validate the temporal query if type or attrs are not present`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        parseQueryAndTemporalParams(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXT,
            true
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("Either type or attrs need to be present in request parameters", it.message)
        }
    }

    @Test
    fun `it should not validate the temporal query if timerel is present without time`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "before")

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        parseQueryAndTemporalParams(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXT
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

        parseQueryAndTemporalParams(
            pagination,
            queryParams,
            APIC_COMPOUND_CONTEXT,
            true
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals("'timerel' and 'time' must be used in conjunction", it.message)
        }
    }

    @Test
    fun `it should parse a valid temporal query`() = runTest {
        val queryParams = gimmeTemporalEntitiesQueryParams()

        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        val temporalEntitiesQuery =
            parseQueryAndTemporalParams(pagination, queryParams, APIC_COMPOUND_CONTEXT).shouldSucceedAndResult()

        assertEquals(
            setOf("urn:ngsi-ld:BeeHive:TESTC".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
            temporalEntitiesQuery.entitiesQuery.ids
        )
        assertEquals("$BEEHIVE_TYPE,$APIARY_TYPE", temporalEntitiesQuery.entitiesQuery.type)
        assertEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), temporalEntitiesQuery.entitiesQuery.attrs)
        assertEquals(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.BETWEEN,
                timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                endTimeAt = ZonedDateTime.parse("2019-10-18T07:31:39Z")
            ),
            temporalEntitiesQuery.temporalQuery
        )
        assertTrue(temporalEntitiesQuery.withTemporalValues)
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

        val temporalEntitiesQuery =
            parseQueryAndTemporalParams(pagination, queryParams, APIC_COMPOUND_CONTEXT).shouldSucceedAndResult()

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

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "outgoing")

        val temporalQuery =
            parseQueryAndTemporalParams(pagination, queryParams, APIC_COMPOUND_CONTEXT).shouldSucceedAndResult()

        assertEquals(1, temporalQuery.entitiesQuery.attrs.size)
        assertTrue(temporalQuery.entitiesQuery.attrs.contains(OUTGOING_PROPERTY))
    }

    @Test
    fun `it should parse a temporal query containing two attrs parameter`() = runTest {
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("attrs", "incoming,outgoing")

        val temporalQuery =
            parseQueryAndTemporalParams(pagination, queryParams, APIC_COMPOUND_CONTEXT).shouldSucceedAndResult()

        assertEquals(2, temporalQuery.entitiesQuery.attrs.size)
        assertIterableEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), temporalQuery.entitiesQuery.attrs)
    }

    @Test
    fun `it should parse a temporal query containing no attrs parameter`() = runTest {
        val pagination = mockkClass(ApplicationProperties.Pagination::class)
        every { pagination.limitDefault } returns 30
        every { pagination.limitMax } returns 100

        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")

        val temporalQuery =
            parseQueryAndTemporalParams(pagination, queryParams, APIC_COMPOUND_CONTEXT).shouldSucceedAndResult()
        assertTrue(temporalQuery.entitiesQuery.attrs.isEmpty())
    }

    @Test
    fun `it should parse lastN parameter if it is a positive integer`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "2")

        val temporalQuery = buildTemporalQuery(queryParams).shouldSucceedAndResult()

        assertEquals(2, temporalQuery.lastN)
    }

    @Test
    fun `it should ignore lastN parameter if it is not an integer`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "A")

        val temporalQuery = buildTemporalQuery(queryParams).shouldSucceedAndResult()

        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should ignore lastN parameter if it is not a positive integer`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2019-10-17T07:31:39Z")
        queryParams.add("lastN", "-2")

        val temporalQuery = buildTemporalQuery(queryParams).shouldSucceedAndResult()

        assertNull(temporalQuery.lastN)
    }

    @Test
    fun `it should treat time and timerel properties as optional in a temporal query`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()

        val temporalQuery = buildTemporalQuery(queryParams).shouldSucceedAndResult()

        assertNull(temporalQuery.timeAt)
        assertNull(temporalQuery.timerel)
    }

    @Test
    fun `it should parse a temporal query containing a timeproperty parameter`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timeproperty", "createdAt")

        val temporalQuery = buildTemporalQuery(queryParams).shouldSucceedAndResult()

        assertEquals(AttributeInstance.TemporalProperty.CREATED_AT, temporalQuery.timeproperty)
    }

    @Test
    fun `it should set timeproperty to observedAt if no value is provided in query parameters`() = runTest {
        val queryParams = LinkedMultiValueMap<String, String>()

        val temporalQuery = buildTemporalQuery(queryParams).shouldSucceedAndResult()

        assertEquals(AttributeInstance.TemporalProperty.OBSERVED_AT, temporalQuery.timeproperty)
    }
}
