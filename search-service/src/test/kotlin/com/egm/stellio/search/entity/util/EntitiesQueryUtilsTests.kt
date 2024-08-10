package com.egm.stellio.search.entity.util

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.common.model.Query.Companion.invoke
import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.support.buildDefaultPagination
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.util.APIARY_TYPE
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.GEO_QUERY_GEOREL_EQUALS
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import java.net.URI

@ActiveProfiles("test")
class EntitiesQueryUtilsTests {

    @Test
    fun `it should parse query parameters`() = runTest {
        val requestParams = gimmeEntitiesQueryParams()
        val entitiesQuery = composeEntitiesQuery(
            buildDefaultPagination(1, 20),
            requestParams,
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedAndResult()

        assertEquals("$BEEHIVE_TYPE,$APIARY_TYPE", entitiesQuery.typeSelection)
        assertEquals(setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY), entitiesQuery.attrs)
        assertEquals(
            setOf("urn:ngsi-ld:BeeHive:TESTC".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
            entitiesQuery.ids
        )
        assertEquals(".*BeeHive.*", entitiesQuery.idPattern)
        assertEquals("brandName!=Mercedes", entitiesQuery.q)
        assertEquals(setOf("urn:ngsi-ld:Dataset:Test1", "urn:ngsi-ld:Dataset:Test2"), entitiesQuery.datasetId)
        assertEquals(true, entitiesQuery.paginationQuery.count)
        assertEquals(1, entitiesQuery.paginationQuery.offset)
        assertEquals(10, entitiesQuery.paginationQuery.limit)
    }

    @Test
    fun `it should decode q in query parameters`() = runTest {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("q", "speed%3E50%3BfoodName%3D%3Ddietary+fibres")
        val entitiesQuery = composeEntitiesQuery(
            buildDefaultPagination(30, 100),
            requestParams,
            NGSILD_TEST_CORE_CONTEXTS
        ).shouldSucceedAndResult()

        assertEquals("speed>50;foodName==dietary fibres", entitiesQuery.q)
    }

    @Test
    fun `it should set default values in query parameters`() = runTest {
        val requestParams = LinkedMultiValueMap<String, String>()
        val entitiesQuery = composeEntitiesQuery(
            buildDefaultPagination(30, 100),
            requestParams,
            NGSILD_TEST_CORE_CONTEXTS
        ).shouldSucceedAndResult()

        assertEquals(null, entitiesQuery.typeSelection)
        assertEquals(emptySet<String>(), entitiesQuery.attrs)
        assertEquals(emptySet<URI>(), entitiesQuery.ids)
        assertEquals(null, entitiesQuery.idPattern)
        assertEquals(null, entitiesQuery.q)
        assertEquals(emptySet<String>(), entitiesQuery.datasetId)
        assertEquals(false, entitiesQuery.paginationQuery.count)
        assertEquals(0, entitiesQuery.paginationQuery.offset)
        assertEquals(30, entitiesQuery.paginationQuery.limit)
    }

    private fun gimmeEntitiesQueryParams(): LinkedMultiValueMap<String, String> {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("type", "BeeHive,Apiary")
        requestParams.add("attrs", "incoming,outgoing")
        requestParams.add("id", "urn:ngsi-ld:BeeHive:TESTC,urn:ngsi-ld:BeeHive:TESTB")
        requestParams.add("idPattern", ".*BeeHive.*")
        requestParams.add("q", "brandName!=Mercedes")
        requestParams.add("datasetId", "urn:ngsi-ld:Dataset:Test1,urn:ngsi-ld:Dataset:Test2")
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
                "scopeQ": "/Nantes",
                "datasetId": ["urn:ngsi-ld:Dataset:Test1", "urn:ngsi-ld:Dataset:Test2"]
            }
        """.trimIndent()

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedWith {
            assertEquals(setOf("urn:ngsi-ld:BeeHive:TESTC".toUri()), it.ids)
            assertEquals("urn:ngsi-ld:BeeHive:*", it.idPattern)
            assertEquals(BEEHIVE_TYPE, it.typeSelection)
            assertEquals(setOf("${NGSILD_DEFAULT_VOCAB}attr1", "${NGSILD_DEFAULT_VOCAB}attr2"), it.attrs)
            assertEquals("temperature>32", it.q)
            assertEquals(GeoQuery.GeometryType.POINT, it.geoQuery?.geometry)
            assertEquals("[1.0, 1.0]", it.geoQuery?.coordinates)
            assertEquals(GEO_QUERY_GEOREL_EQUALS, it.geoQuery?.georel)
            assertEquals(NGSILD_OBSERVATION_SPACE_PROPERTY, it.geoQuery?.geoproperty)
            assertEquals("/Nantes", it.scopeQ)
            assertEquals(setOf("urn:ngsi-ld:Dataset:Test1", "urn:ngsi-ld:Dataset:Test2"), it.datasetId)
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

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedWith {
            assertEquals(BEEHIVE_TYPE, it.typeSelection)
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

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
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

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
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

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldFailWith {
            it is BadRequestDataException &&
                it.message.startsWith("The supplied query could not be parsed")
        }
    }

    private fun composeEntitiesQueryFromPostRequest(
        defaultPagination: ApplicationProperties.Pagination,
        requestBody: String,
        requestParams: MultiValueMap<String, String>,
        contexts: List<String>
    ): Either<APIException, EntitiesQuery> = either {
        val query = Query(requestBody).bind()
        composeEntitiesQueryFromPostRequest(
            defaultPagination,
            query,
            requestParams,
            contexts
        ).bind()
    }
}
