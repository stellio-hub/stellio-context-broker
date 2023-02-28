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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
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

    val entity01Uri = "urn:ngsi-ld:BeeHive:01".toUri()
    val entity02Uri = "urn:ngsi-ld:BeeHive:02".toUri()

    @BeforeAll
    fun createEntities() {
        val firstRawEntity = loadSampleData("entity_with_all_attributes_1.jsonld")
        val secondRawEntity = loadSampleData("entity_with_all_attributes_2.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        runBlocking {
            entityPayloadService.createEntity(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            entityPayloadService.createEntity(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        }
    }

    @AfterAll
    fun deleteEntities() {
        runBlocking {
            entityPayloadService.deleteEntityPayload(entity01Uri)
            entityPayloadService.deleteEntityPayload(entity02Uri)
        }
    }

    @Test
    fun `it should retrieve entities according to ids and types and attrs`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    ids = setOf(entity02Uri, entity01Uri),
                    types = setOf(BEEHIVE_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(2, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri, entity02Uri)
    }

    @Test
    fun `it should retrieve entities according to attrs and types`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    types = setOf(BEEHIVE_TYPE),
                    attrs = setOf(NGSILD_NAME_PROPERTY),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri)
    }

    @Test
    fun `it should retrieve entities by a list of ids`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    ids = setOf(entity02Uri),
                    types = setOf(BEEHIVE_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity02Uri)
    }

    @Test
    fun `it should retrieve entities with respect to limit and offset`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 1,
                    types = setOf(BEEHIVE_TYPE),
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
                    types = setOf(BEEHIVE_TYPE),
                    idPattern = ".*urn:ngsi-ld:BeeHive:01.*",
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri)
    }

    @ParameterizedTest
    @CsvSource(
        "integer==213, 1, urn:ngsi-ld:BeeHive:01",
        "relationship==urn:ngsi-ld:AnotherEntity:01, 1, urn:ngsi-ld:BeeHive:01",
        "(float==21.34|float==44.75), 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==213;boolean==true, 1, urn:ngsi-ld:BeeHive:01",
        "(integer>200|integer<100);observedProperty.observedAt<2023-02-25T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "string~=\"(?i)another.*\", 1, urn:ngsi-ld:BeeHive:02",
        "string!~=\"(?i)another.*\", 1, urn:ngsi-ld:BeeHive:01",
        "dateTime==2023-02-16T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "dateTime~=2023-02-16T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "dateTime>2023-02-16T00:00:00Z, 1, urn:ngsi-ld:BeeHive:02",
        "observedProperty.observedAt>2023-02-25T00:00:00Z, 1, urn:ngsi-ld:BeeHive:02",
        "observedProperty.observedAt>2023-02-01T00:00:00Z, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "name.createdAt>2023-02-01T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "propertyWithMetadata.unitCode==\"MTR\", 1, urn:ngsi-ld:BeeHive:02",
        "propertyWithMetadata.license==\"GPL\", 1, urn:ngsi-ld:BeeHive:02",
        "multiInstanceProperty.datasetId==urn:ngsi-ld:Dataset:01, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "multiInstanceProperty.datasetId==urn:ngsi-ld:Dataset:02, 1, urn:ngsi-ld:BeeHive:01",
        "jsonObject[aString]==\"flow monitoring\", 1, urn:ngsi-ld:BeeHive:02",
        "jsonObject[aNumber]==93.93, 1, urn:ngsi-ld:BeeHive:02",
        "jsonObject[anObject.name]==\"River\", 1, urn:ngsi-ld:BeeHive:02",
        "integer==143..213, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==144..213, 1, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==100..120, 0, ",
        "listOfString==\"iot\", 1, urn:ngsi-ld:BeeHive:01",
        "listOfString==\"stellio\", 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'listOfString==\"iot\",\"dataviz\"', 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'listOfInt==12,14', 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'listOfInt==12', 1, urn:ngsi-ld:BeeHive:01"
    )
    fun `it should retrieve entities according to q parameter`(
        q: String,
        expectedCount: Int,
        expectedListOfEntities: String?
    ) = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 2,
                    q = q,
                    types = setOf(BEEHIVE_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(expectedCount, entitiesIds.size)
        if (expectedListOfEntities != null)
            assertThat(expectedListOfEntities.split(",")).containsAll(entitiesIds.toListOfString())
    }

    @Test
    fun `it should retrieve entities according to access rights`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    types = setOf(BEEHIVE_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) {
                """
                (
                    (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                    OR
                    (entity_payload.entity_id IN ('urn:ngsi-ld:BeeHive:01'))
                )
                """.trimIndent()
            }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri)
    }

    @Test
    fun `it should retrieve entities according to specific access policy`() = runTest {
        updateSpecificAccessPolicy(entity01Uri, AuthContextModel.SpecificAccessPolicy.AUTH_READ)

        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    types = setOf(BEEHIVE_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) {
                """
                (
                    (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                    OR
                    (entity_payload.entity_id IN ('urn:ngsi-ld:BeeHive:03'))
                )
                """.trimIndent()
            }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri)

        resetSpecificAccessPolicy(entity01Uri)
    }

    @Test
    fun `it should retrieve entities according to specific access policy and rights`() = runTest {
        updateSpecificAccessPolicy(entity01Uri, AuthContextModel.SpecificAccessPolicy.AUTH_READ)

        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 0,
                    limit = 30,
                    types = setOf(BEEHIVE_TYPE),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) {
                """
                (
                    (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                    OR
                    (entity_payload.entity_id IN ('urn:ngsi-ld:BeeHive:02'))
                )
                """.trimIndent()
            }

        assertEquals(2, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri, entity02Uri)

        resetSpecificAccessPolicy(entity01Uri)
    }

    @Test
    fun `it should retrieve the count of entities`() = runTest {
        entityPayloadService.queryEntitiesCount(
            QueryParams(
                offset = 0,
                limit = 30,
                types = setOf(BEEHIVE_TYPE),
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
                ids = setOf(entity02Uri, entity01Uri),
                types = setOf(BEEHIVE_TYPE),
                context = APIC_COMPOUND_CONTEXT
            )
        ) { "entity_payload.entity_id IN ('urn:ngsi-ld:BeeHive:01')" }
            .shouldSucceedWith { assertEquals(1, it) }
    }

    @Test
    fun `it should return an empty list if no entity matches the requested type`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    offset = 10,
                    limit = 2,
                    ids = setOf(entity02Uri, entity01Uri),
                    types = setOf("https://ontology.eglobalmark.com/apic#UnknownType"),
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

    private fun resetSpecificAccessPolicy(
        entityId: URI
    ) = r2dbcEntityTemplate.update(
        Query.query(Criteria.where("entity_id").`is`(entityId)),
        Update.update("specific_access_policy", null),
        EntityPayload::class.java
    ).block()
}
