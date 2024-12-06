package com.egm.stellio.search.entity.util

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.search.common.model.Query
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.search.support.buildDefaultPagination
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.queryparameter.GeoQuery
import com.egm.stellio.shared.queryparameter.Georel
import com.egm.stellio.shared.queryparameter.LinkedEntityQuery.Companion.JoinType
import com.egm.stellio.shared.util.APIARY_TYPE
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.BEEKEEPER_TYPE
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVATION_SPACE_PROPERTY
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXTS
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
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
        val entitiesQuery = composeEntitiesQueryFromGet(
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
        assertEquals(
            setOf("urn:ngsi-ld:Beekeper:A".toUri(), "urn:ngsi-ld:Beekeeper:B".toUri()),
            entitiesQuery.linkedEntityQuery?.containedBy
        )
        assertEquals(JoinType.INLINE, entitiesQuery.linkedEntityQuery?.join)
        assertEquals(1.toUInt(), entitiesQuery.linkedEntityQuery?.joinLevel)
    }

    @Test
    fun `it should decode q in query parameters`() = runTest {
        val requestParams = LinkedMultiValueMap<String, String>()
        requestParams.add("q", "speed%3E50%3BfoodName%3D%3Ddietary+fibres")
        val entitiesQuery = composeEntitiesQueryFromGet(
            buildDefaultPagination(30, 100),
            requestParams,
            NGSILD_TEST_CORE_CONTEXTS
        ).shouldSucceedAndResult()

        assertEquals("speed>50;foodName==dietary fibres", entitiesQuery.q)
    }

    @Test
    fun `it should set default values in query parameters`() = runTest {
        val requestParams = LinkedMultiValueMap<String, String>()
        val entitiesQuery = composeEntitiesQueryFromGet(
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

    @Test
    fun `it should return a BadRequest error if join parameter is not recognized`() = runTest {
        composeEntitiesQueryFromGet(
            buildDefaultPagination(30, 100),
            LinkedMultiValueMap<String, String>().apply {
                add("join", "unknown")
            },
            NGSILD_TEST_CORE_CONTEXTS
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "'unknown' is not a recognized value for 'join' parameter " +
                    "(only 'flat', 'inline' and '@none' are allowed)",
                it.message
            )
        }
    }

    @Test
    fun `it should return a BadRequest error if joinLevel parameter is not a positive integer`() = runTest {
        composeEntitiesQueryFromGet(
            buildDefaultPagination(30, 100),
            LinkedMultiValueMap<String, String>().apply {
                add("join", "flat")
                add("joinLevel", "-1")
            },
            NGSILD_TEST_CORE_CONTEXTS
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "'-1' is not a recognized value for 'joinLevel' parameter (only positive integers are allowed)",
                it.message
            )
        }
    }

    @Test
    fun `it should return a BadRequest error if join is not specified and joinLevel is specified`() = runTest {
        composeEntitiesQueryFromGet(
            buildDefaultPagination(30, 100),
            LinkedMultiValueMap<String, String>().apply {
                add("joinLevel", "1")
            },
            NGSILD_TEST_CORE_CONTEXTS
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "'join' must be specified if 'joinLevel' or 'containedBy' are specified",
                it.message
            )
        }
    }

    @Test
    fun `it should return a BadRequest error if join is not specified and containedBy is specified`() = runTest {
        composeEntitiesQueryFromGet(
            buildDefaultPagination(30, 100),
            LinkedMultiValueMap<String, String>().apply {
                add("containedBy", "urn:ngsi-ld:BeeHive:TESTA,urn:ngsi-ld:BeeHive:TESTC")
            },
            NGSILD_TEST_CORE_CONTEXTS
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "'join' must be specified if 'joinLevel' or 'containedBy' are specified",
                it.message
            )
        }
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
        requestParams.add("containedBy", "urn:ngsi-ld:Beekeper:A,urn:ngsi-ld:Beekeeper:B")
        requestParams.add("join", "inline")
        requestParams.add("joinLevel", "1")
        return requestParams
    }

    @Test
    fun `it should parse a valid complete Query datatype`() = runTest {
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
                "datasetId": ["urn:ngsi-ld:Dataset:Test1", "urn:ngsi-ld:Dataset:Test2"],
                "join": "flat",
                "joinLevel": "2",
                "containedBy": ["urn:ngsi-ld:BeeHive:TESTA", "urn:ngsi-ld:BeeHive:TESTB"]
            }
        """.trimIndent()

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedWith {
            assertNotNull(it.entitySelectors)
            assertEquals(1, it.entitySelectors!!.size)
            val entitySelector = it.entitySelectors[0]
            assertEquals("urn:ngsi-ld:BeeHive:TESTC".toUri(), entitySelector.id)
            assertEquals("urn:ngsi-ld:BeeHive:*", entitySelector.idPattern)
            assertEquals(BEEHIVE_TYPE, entitySelector.typeSelection)
            assertEquals(setOf("${NGSILD_DEFAULT_VOCAB}attr1", "${NGSILD_DEFAULT_VOCAB}attr2"), it.attrs)
            assertEquals("temperature>32", it.q)
            assertEquals(GeoQuery.GeometryType.POINT, it.geoQuery?.geometry)
            assertEquals("[1.0, 1.0]", it.geoQuery?.coordinates)
            assertEquals(Georel.EQUALS.key, it.geoQuery?.georel)
            assertEquals(NGSILD_OBSERVATION_SPACE_PROPERTY, it.geoQuery?.geoproperty)
            assertEquals("/Nantes", it.scopeQ)
            assertEquals(setOf("urn:ngsi-ld:Dataset:Test1", "urn:ngsi-ld:Dataset:Test2"), it.datasetId)
            assertEquals(JoinType.FLAT, it.linkedEntityQuery?.join)
            assertEquals(2, it.linkedEntityQuery?.joinLevel?.toInt())
            assertEquals(
                setOf("urn:ngsi-ld:BeeHive:TESTA".toUri(), "urn:ngsi-ld:BeeHive:TESTB".toUri()),
                it.linkedEntityQuery?.containedBy
            )
        }
    }

    @Test
    fun `it should parse a valid simple Query datatype`() = runTest {
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
            assertEquals(BEEHIVE_TYPE, it.entitySelectors!![0].typeSelection)
            assertEquals(setOf("${NGSILD_DEFAULT_VOCAB}attr1"), it.attrs)
            assertEquals("temperature>32", it.q)
        }
    }

    @Test
    fun `it should parse a Query datatype with more than one entity selectors`() = runTest {
        val query = """
            {
                "type": "Query",
                "entities": [
                    {
                        "idPattern": "urn:ngsi-ld:BeeHive:*",
                        "type": "BeeHive"
                    },
                    {
                        "type": "Apiary"
                    },                    
                    {
                        "id": "urn:ngsi-ld:Beekeeper:Jules",
                        "type": "Beekeeper"
                    }
                ]
            }
        """.trimIndent()

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedWith {
            assertThat(it.entitySelectors)
                .hasSize(3)
                .containsAll(
                    listOf(
                        EntitySelector(id = null, idPattern = "urn:ngsi-ld:BeeHive:*", typeSelection = BEEHIVE_TYPE),
                        EntitySelector(id = null, idPattern = null, typeSelection = APIARY_TYPE),
                        EntitySelector(
                            id = "urn:ngsi-ld:Beekeeper:Jules".toUri(),
                            idPattern = null,
                            typeSelection = BEEKEEPER_TYPE
                        )
                    )
                )
        }
    }

    @Test
    fun `it should not validate a Query if the type is not correct`() {
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
    fun `it should not validate a Query if the payload could not be parsed because the JSON is invalid`() {
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
    fun `it should not validate a Query if the payload contains unexpected parameters`() {
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

    @Test
    fun `it should not validate a Query if an entity selector has no type member`() {
        val query = """
            {
                "type": "Query",
                "entities": [
                    {
                        "idPattern": "urn:ngsi-ld:BeeHive:*"
                    }
                ]
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
    fun `it should not validate a Query if joinLevel is invalid`() {
        val query = """
            {
                "type": "Query",
                "joinLevel": -1
            }
        """.trimIndent()

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                "'-1' is not a recognized value for 'joinLevel' parameter (only positive integers are allowed)",
                it.message
            )
        }
    }

    @Test
    fun `it should not validate a Query if join is invalid`() {
        val query = """
            {
                "type": "Query",
                "join": "unknown"
            }
        """.trimIndent()

        composeEntitiesQueryFromPostRequest(
            buildDefaultPagination(30, 100),
            query,
            LinkedMultiValueMap(),
            APIC_COMPOUND_CONTEXTS
        ).shouldFail {
            assertInstanceOf(BadRequestDataException::class.java, it)
            assertEquals(
                """
                    'unknown' is not a recognized value for 'join' parameter (only 'flat', 'inline' and '@none' are allowed)
                """.trimIndent(),
                it.message
            )
        }
    }

    private fun composeEntitiesQueryFromPostRequest(
        defaultPagination: ApplicationProperties.Pagination,
        requestBody: String,
        requestParams: MultiValueMap<String, String>,
        contexts: List<String>
    ): Either<APIException, EntitiesQueryFromPost> = either {
        val query = Query(requestBody).bind()
        composeEntitiesQueryFromPost(
            defaultPagination,
            query,
            requestParams,
            contexts
        ).bind()
    }
}
