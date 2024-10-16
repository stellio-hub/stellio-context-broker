package com.egm.stellio.search.temporal.service

import arrow.core.None
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.service.EntityAttributeService
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.scope.ScopeService
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.EMPTY_PAYLOAD
import com.egm.stellio.search.support.buildDefaultQueryParams
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.support.gimmeEntityPayload
import com.egm.stellio.search.temporal.model.AttributeInstanceResult
import com.egm.stellio.search.temporal.model.FullAttributeInstanceResult
import com.egm.stellio.search.temporal.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIARY_TYPE
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_TERM
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.ZonedDateTime

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [TemporalQueryService::class])
@ActiveProfiles("test")
@ExperimentalCoroutinesApi
class TemporalQueryServiceTests {

    @Autowired
    private lateinit var temporalQueryService: TemporalQueryService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var scopeService: ScopeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @MockkBean
    private lateinit var entityAttributeService: EntityAttributeService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    private val now = ngsiLdDateTime()

    private val entityUri = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @Test
    fun `it should return an API exception if the entity does not exist`() = runTest {
        coEvery {
            entityQueryService.checkEntityExistence(any())
        } returns ResourceNotFoundException(entityNotFoundMessage(entityUri.toString())).left()

        temporalQueryService.queryTemporalEntity(
            entityUri,
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = buildDefaultTestTemporalQuery(),
                withTemporalValues = true,
                withAudit = false,
                withAggregatedValues = false
            )
        ).fold({
            assertInstanceOf(ResourceNotFoundException::class.java, it)
            assertEquals("Entity $entityUri was not found", it.message)
        }, {
            fail("it should have returned an API exception if the entity does not exist")
        })
    }

    @Test
    fun `it should query a temporal entity as requested by query params`() = runTest {
        val attributes =
            listOf(INCOMING_PROPERTY, OUTGOING_PROPERTY).map {
                Attribute(
                    entityId = entityUri,
                    attributeName = it,
                    attributeValueType = Attribute.AttributeValueType.NUMBER,
                    createdAt = now,
                    payload = EMPTY_JSON_PAYLOAD
                )
            }

        coEvery { entityQueryService.checkEntityExistence(any()) } returns Unit.right()
        coEvery { authorizationService.userCanReadEntity(any(), any()) } returns Unit.right()
        coEvery { entityAttributeService.getForEntity(any(), any(), any()) } returns attributes
        coEvery { entityQueryService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<Attribute>>())
        } returns
            listOf(
                FullAttributeInstanceResult(attributes[0].id, EMPTY_PAYLOAD, now, NGSILD_CREATED_AT_TERM, null),
                FullAttributeInstanceResult(attributes[1].id, EMPTY_PAYLOAD, now, NGSILD_CREATED_AT_TERM, null)
            ).right()

        temporalQueryService.queryTemporalEntity(
            entityUri,
            TemporalEntitiesQueryFromGet(
                temporalQuery = buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                entitiesQuery = EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            )
        )

        coVerify {
            entityQueryService.checkEntityExistence(entityUri)
            authorizationService.userCanReadEntity(entityUri, None)
            entityAttributeService.getForEntity(entityUri, emptySet(), emptySet())
            attributeInstanceService.search(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.AFTER &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        )
                },
                any<List<Attribute>>()
            )
            scopeService.retrieveHistory(listOf(entityUri), any())
        }
    }

    @Test
    fun `it should not return an oldest timestamp if not in an aggregattion query`() = runTest {
        val origin = temporalQueryService.calculateOldestTimestamp(
            entityUri,
            TemporalEntitiesQueryFromGet(
                temporalQuery = buildDefaultTestTemporalQuery(),
                entitiesQuery = EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            emptyList()
        )

        assertNull(origin)
    }

    @Test
    fun `it should return timeAt as the oldest timestamp if it is provided in the temporal query`() = runTest {
        val origin = temporalQueryService.calculateOldestTimestamp(
            entityUri,
            TemporalEntitiesQueryFromGet(
                temporalQuery = buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = now
                ),
                entitiesQuery = EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            emptyList()
        )

        assertNotNull(origin)
        assertEquals(now, origin)
    }

    @Test
    fun `it should return the oldest timestamp from the DB if none is provided in the temporal query`() = runTest {
        coEvery {
            attributeInstanceService.selectOldestDate(any(), any())
        } returns ZonedDateTime.parse("2023-09-03T12:34:56Z")
        coEvery {
            scopeService.selectOldestDate(any(), any())
        } returns ZonedDateTime.parse("2023-09-03T14:22:55Z")

        val origin = temporalQueryService.calculateOldestTimestamp(
            entityUri,
            TemporalEntitiesQueryFromGet(
                temporalQuery = buildDefaultTestTemporalQuery(),
                entitiesQuery = EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 0, offset = 50),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            emptyList()
        )

        assertNotNull(origin)
        assertEquals(ZonedDateTime.parse("2023-09-03T12:34:56Z"), origin)
    }

    @Test
    fun `it should query temporal entities as requested by query params`() = runTest {
        val attribute = Attribute(
            entityId = entityUri,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery { entityQueryService.queryEntities(any(), any<() -> String?>()) } returns listOf(entityUri)
        coEvery {
            entityAttributeService.getForEntities(any(), any())
        } returns listOf(attribute)
        coEvery { entityQueryService.queryEntitiesCount(any(), any()) } returns 1.right()
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery { entityQueryService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<Attribute>>())
        } returns
            listOf(
                SimplifiedAttributeInstanceResult(
                    attributeUuid = attribute.id,
                    value = 2.0,
                    time = ngsiLdDateTime()
                )
            ).right()

        temporalQueryService.queryTemporalEntities(
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z")
                ),
                withTemporalValues = true,
                withAudit = false,
                withAggregatedValues = false
            )
        )

        coVerify {
            entityAttributeService.getForEntities(
                listOf(entityUri),
                EntitiesQueryFromGet(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            )
            attributeInstanceService.search(
                match { temporalEntitiesQuery ->
                    temporalEntitiesQuery.temporalQuery.timerel == TemporalQuery.Timerel.BEFORE &&
                        temporalEntitiesQuery.temporalQuery.timeAt!!.isEqual(
                            ZonedDateTime.parse("2019-10-17T07:31:39Z")
                        )
                },
                any<List<Attribute>>()
            )
            entityQueryService.queryEntitiesCount(
                EntitiesQueryFromGet(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                any()
            )
            scopeService.retrieveHistory(listOf(entityUri), any())
        }
    }

    @Test
    fun `it should return an empty list for an attribute if it has no temporal values`() = runTest {
        val attribute = Attribute(
            entityId = entityUri,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        coEvery { authorizationService.computeAccessRightFilter(any()) } returns { null }
        coEvery { entityQueryService.queryEntities(any(), any<() -> String?>()) } returns listOf(entityUri)
        coEvery {
            entityAttributeService.getForEntities(any(), any())
        } returns listOf(attribute)
        coEvery { scopeService.retrieveHistory(any(), any()) } returns emptyList<ScopeInstanceResult>().right()
        coEvery {
            attributeInstanceService.search(any(), any<List<Attribute>>())
        } returns emptyList<AttributeInstanceResult>().right()
        coEvery { entityQueryService.retrieve(any<URI>()) } returns gimmeEntityPayload().right()
        coEvery { entityQueryService.queryEntitiesCount(any(), any()) } returns 1.right()

        temporalQueryService.queryTemporalEntities(
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    typeSelection = "$BEEHIVE_TYPE,$APIARY_TYPE",
                    paginationQuery = PaginationQuery(limit = 2, offset = 2),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ZonedDateTime.parse("2019-10-17T07:31:39Z"),
                    aggrMethods = listOf(TemporalQuery.Aggregate.AVG)
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            )
        )
            .fold({
                fail("it should have returned an empty list")
            }, {
                assertThat(it.first).hasSize(1)
                assertJsonPayloadsAreEqual(
                    """
                    {
                        "@id": "urn:ngsi-ld:BeeHive:TESTC",
                        "@type": [
                            "https://ontology.eglobalmark.com/apic#BeeHive"
                        ],
                        "https://uri.etsi.org/ngsi-ld/createdAt": [
                            {
                                "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
                                "@value": "$now"
                            }
                        ],
                        "https://ontology.eglobalmark.com/apic#incoming":[
                            {
                                "@type": [
                                    "https://uri.etsi.org/ngsi-ld/Property"
                                ],
                                "https://uri.etsi.org/ngsi-ld/avg":[
                                    {
                                        "@list":[]
                                    }
                                ]
                            }
                        ]
                    }
                    """.trimIndent(),
                    JsonUtils.serializeObject(it.first[0].members),
                    setOf(NGSILD_CREATED_AT_PROPERTY)
                )
            })
    }

    private fun gimmeEntityPayload() =
        gimmeEntityPayload(
            entityId = entityUri,
            payload = loadSampleData("beehive_expanded.jsonld")
        )
}