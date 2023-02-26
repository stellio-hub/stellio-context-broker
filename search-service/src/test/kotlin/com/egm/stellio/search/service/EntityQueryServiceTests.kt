package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.data.relational.core.query.Update
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class EntityQueryServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityPayloadService: EntityPayloadService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    val beehiveTestDId = "urn:ngsi-ld:BeeHive:TESTD".toUri()

    @BeforeAll
    fun createEntities() {
        val firstRawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")
        val secondRawEntity = loadSampleData("beehive.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        runBlocking {
            entityPayloadService.createEntity(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            entityPayloadService.createEntity(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        }
    }

    @AfterAll
    fun deleteEntities() {
        runBlocking {
            entityPayloadService.deleteEntityPayload(beehiveTestCId)
            entityPayloadService.deleteEntityPayload(beehiveTestDId)
        }
    }

    @Test
    fun `it should retrieve entities according to ids, types and attrs`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(2, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestCId, beehiveTestDId)
    }

    @Test
    fun `it should retrieve entities according to query (q by equal query)`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    q = "incoming==1543",
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(2, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestCId, beehiveTestDId)
    }

    @Test
    fun `it should retrieve entities according to query (q by regex query)`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    q = "name~=\"(?i)paris.*\"",
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(NGSILD_NAME_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestCId)
    }

    @Test
    fun `it should retrieve entities according to query (q with datetime value)`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    q = "dateOfFirstBee==2018-12-04T12:00:00.00Z",
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(DATE_OF_FIRST_BEE_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestCId)
    }

    @Test
    fun `it should not retrieve entities when doing a regex query on a datetime`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    q = "dateOfFirstBee=~2018-12-04T12:00:00.00Z",
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(DATE_OF_FIRST_BEE_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(0, entitiesIds.size)
    }

    @Test
    fun `it should retrieve entities by a list of ids`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    ids = setOf(beehiveTestDId),
                    types = setOf(BEEHIVE_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestDId)
    }

    @Test
    fun `it should retrieve entities with respect to limit and offset`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
    }

    @Test
    fun `it should retrieve entities with respect to idPattern`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    idPattern = ".*urn:ngsi-ld:BeeHive:TESTD.*",
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestDId)
    }

    @Test
    fun `it should retrieve entities according to access rights`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) {
                """
                (
                    (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                    OR
                    (tea.entity_id IN ('urn:ngsi-ld:BeeHive:TESTD'))
                )
                """.trimIndent()
            }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestDId)
    }

    @Test
    fun `it should retrieve entities according to specific access policy`() = runTest {
        updateSpecificAccessPolicy(beehiveTestCId, AuthContextModel.SpecificAccessPolicy.AUTH_READ)

        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) {
                """
                (
                    (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                    OR
                    (tea.entity_id IN ('urn:ngsi-ld:BeeHive:TESTE'))
                )
                """.trimIndent()
            }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestCId)

        restSpecificAccessPolicy(beehiveTestCId)
    }

    @Test
    fun `it should retrieve entities according to specific access policy and rights`() = runTest {
        updateSpecificAccessPolicy(beehiveTestCId, AuthContextModel.SpecificAccessPolicy.AUTH_READ)

        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) {
                """
                (
                    (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                    OR
                    (tea.entity_id IN ('urn:ngsi-ld:BeeHive:TESTD'))
                )
                """.trimIndent()
            }

        assertEquals(2, entitiesIds.size)
        assertThat(entitiesIds).contains(beehiveTestCId, beehiveTestDId)

        restSpecificAccessPolicy(beehiveTestCId)
    }

    @Test
    fun `it should retrieve the count of entities`() = runTest {
        entityPayloadService.queryEntitiesCount(
            QueryParams(
                offset = 0,
                limit = 30,
                ids = setOf(beehiveTestDId, beehiveTestCId),
                types = setOf(BEEHIVE_TYPE),
                attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                context = APIC_COMPOUND_CONTEXT
            )
        ) { null }.shouldSucceedWith { assertEquals(2, it) }
    }

    @Test
    fun `it should retrieve the count of entities according to access rights`() = runTest {
        entityPayloadService.queryEntitiesCount(
            QueryParams(
                offset = 0,
                limit = 30,
                ids = setOf(beehiveTestDId, beehiveTestCId),
                types = setOf(BEEHIVE_TYPE),
                context = APIC_COMPOUND_CONTEXT
            )
        ) { "tea.entity_id IN ('urn:ngsi-ld:BeeHive:TESTC')" }
            .shouldSucceedWith { assertEquals(1, it) }
    }

    @Test
    fun `it should return an empty list if no entity matches the requested type`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 10,
                    limit = 2,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf("https://ontology.eglobalmark.com/apic#UnknownType"),
                    attrs = setOf(INCOMING_PROPERTY, OUTGOING_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertThat(entitiesIds).isEmpty()
    }

    @Test
    fun `it should return an empty list if no entitiy matched the requested attributes`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 10,
                    limit = 2,
                    ids = setOf(beehiveTestDId, beehiveTestCId),
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf("unknownAttribute"),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertThat(entitiesIds).isEmpty()
    }

    private fun updateSpecificAccessPolicy(
        entityId: URI,
        specificAccessPolicy: AuthContextModel.SpecificAccessPolicy
    ) = r2dbcEntityTemplate.update(
        Query.query(Criteria.where("entity_id").`is`(entityId)),
        Update.update("specific_access_policy", specificAccessPolicy.toString()),
        EntityPayload::class.java
    ).block()

    private fun restSpecificAccessPolicy(
        entityId: URI
    ) = r2dbcEntityTemplate.update(
        Query.query(Criteria.where("entity_id").`is`(entityId)),
        Update.update("specific_access_policy", null),
        EntityPayload::class.java
    ).block()
}
