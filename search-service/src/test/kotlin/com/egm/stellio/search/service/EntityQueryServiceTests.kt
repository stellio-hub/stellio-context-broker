package com.egm.stellio.search.service

import arrow.core.right
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.GeoQuery
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
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

    @MockkBean(relaxed = true)
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    val entity01Uri = "urn:ngsi-ld:BeeHive:01".toUri()
    val entity02Uri = "urn:ngsi-ld:BeeHive:02".toUri()
    val entity03Uri = "urn:ngsi-ld:MultiTypes:03".toUri()
    val entity04Uri = "urn:ngsi-ld:Beekeeper:04".toUri()
    val entity05Uri = "urn:ngsi-ld:Apiary:05".toUri()

    @BeforeAll
    fun createEntities() {
        val firstRawEntity = loadSampleData("entity_with_all_attributes_1.jsonld")
        val secondRawEntity = loadSampleData("entity_with_all_attributes_2.jsonld")
        val thirdRawEntity = loadSampleData("entity_with_multi_types.jsonld")
        val fourthRawEntity = loadSampleData("beekeeper.jsonld")
        val fifthRawEntity = loadSampleData("apiary.jsonld")

        coEvery { attributeInstanceService.create(any()) } returns Unit.right()

        runBlocking {
            entityPayloadService.createEntity(firstRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            entityPayloadService.createEntity(secondRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            entityPayloadService.createEntity(thirdRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            entityPayloadService.createEntity(fourthRawEntity, listOf(APIC_COMPOUND_CONTEXT))
            entityPayloadService.createEntity(fifthRawEntity, listOf(APIC_COMPOUND_CONTEXT))
        }
    }

    @AfterAll
    fun deleteEntities() {
        runBlocking {
            entityPayloadService.deleteEntity(entity01Uri)
            entityPayloadService.deleteEntity(entity02Uri)
            entityPayloadService.deleteEntity(entity03Uri)
            entityPayloadService.deleteEntity(entity04Uri)
            entityPayloadService.deleteEntity(entity05Uri)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "$BEEHIVE_TYPE, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'$APIARY_TYPE|$BEEKEEPER_TYPE', 3, 'urn:ngsi-ld:Apiary:05,urn:ngsi-ld:Beekeeper:04,urn:ngsi-ld:MultiTypes:03'",
        "'$APIARY_TYPE,$BEEKEEPER_TYPE', 3, 'urn:ngsi-ld:Apiary:05,urn:ngsi-ld:Beekeeper:04,urn:ngsi-ld:MultiTypes:03'",
        "$BEEKEEPER_TYPE, 2, 'urn:ngsi-ld:Beekeeper:04,urn:ngsi-ld:MultiTypes:03'",
        "'$BEEKEEPER_TYPE;$SENSOR_TYPE', 1, urn:ngsi-ld:MultiTypes:03",
        "'$BEEKEEPER_TYPE;$DEVICE_TYPE', 0, ",
        "$DEVICE_TYPE, 0, ",
        "$SENSOR_TYPE, 1, urn:ngsi-ld:MultiTypes:03",
        "'($BEEKEEPER_TYPE;$SENSOR_TYPE),$DEVICE_TYPE', 1, urn:ngsi-ld:MultiTypes:03"
    )
    fun `it should retrieve entities according types selection languages`(
        types: String,
        expectedCount: Int,
        expectedListOfEntities: String?
    ) = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    type = types,
                    limit = 30,
                    offset = 0,
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(expectedCount, entitiesIds.size)
        if (expectedListOfEntities != null)
            assertThat(expectedListOfEntities.split(",")).containsAll(entitiesIds.toListOfString())
    }

    @ParameterizedTest
    @CsvSource(
        "/Madrid/Gardens/ParqueNorte, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "/Madrid/+/ParqueNorte, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "/CompanyA/#, 1, urn:ngsi-ld:BeeHive:01",
        "/#, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "/Madrid/Gardens/ParqueNorte;/CompanyA/OrganizationB/UnitC, 1, urn:ngsi-ld:BeeHive:01",
        "'/Madrid/Gardens/ParqueNorte,/CompanyA/OrganizationB/UnitC',2,'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'"
    )
    fun `it should retrieve entities according to scope query`(
        scopeQ: String,
        expectedCount: Int,
        expectedListOfEntities: String?
    ) = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    type = BEEHIVE_TYPE,
                    scopeQ = scopeQ,
                    limit = 30,
                    offset = 0,
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(expectedCount, entitiesIds.size)
        if (expectedListOfEntities != null)
            assertThat(expectedListOfEntities.split(",")).containsAll(entitiesIds.toListOfString())
    }

    @Test
    fun `it should retrieve entities according to ids and types and attrs`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    ids = setOf(entity02Uri, entity01Uri),
                    type = BEEHIVE_TYPE,
                    limit = 2,
                    offset = 0,
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
                    type = BEEHIVE_TYPE,
                    limit = 2,
                    offset = 0,
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
                    ids = setOf(entity02Uri),
                    type = BEEHIVE_TYPE,
                    limit = 1,
                    offset = 0,
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
                    type = BEEHIVE_TYPE,
                    limit = 1,
                    offset = 0,
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
                    type = BEEHIVE_TYPE,
                    idPattern = ".*urn:ngsi-ld:BeeHive:01.*",
                    limit = 1,
                    offset = 0,
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
        "relationship==urn:ngsi-ld:YetAnotherEntity:01, 0, ",
        "(float==21.34|float==44.75), 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==213;boolean==true, 1, urn:ngsi-ld:BeeHive:01",
        "(integer>200|integer<100);observedProperty.observedAt<2023-02-25T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "string~=\"(?i)another.*\", 1, urn:ngsi-ld:BeeHive:02",
        "string!~=\"(?i)another.*\", 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:Apiary:05'",
        "(string!~=\"(?i)another.*\";integer==213), 1, urn:ngsi-ld:BeeHive:01",
        "dateTime==2023-02-16T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "dateTime~=2023-02-16T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "dateTime>2023-02-16T00:00:00Z, 1, urn:ngsi-ld:BeeHive:02",
        "boolean==true, 1, urn:ngsi-ld:BeeHive:01",
        "observedProperty.observedAt>2023-02-25T00:00:00Z, 1, urn:ngsi-ld:BeeHive:02",
        "observedProperty.observedAt>2023-02-01T00:00:00Z, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "observedProperty.observedAt<2023-01-01T00:00:00Z, 0, ",
        "name.createdAt>2023-02-01T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "propertyWithMetadata.unitCode==\"MTR\", 1, urn:ngsi-ld:BeeHive:02",
        "propertyWithMetadata.license==\"GPL\", 1, urn:ngsi-ld:BeeHive:02",
        "propertyWithMetadata.license.since==\"2023\", 1, urn:ngsi-ld:BeeHive:02",
        "multiInstanceProperty.datasetId==urn:ngsi-ld:Dataset:01, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "multiInstanceProperty.datasetId==urn:ngsi-ld:Dataset:02, 1, urn:ngsi-ld:BeeHive:01",
        "multiInstanceProperty.datasetId==urn:ngsi-ld:Dataset:10, 0, ",
        "jsonObject[aString]==\"flow monitoring\", 1, urn:ngsi-ld:BeeHive:02",
        "jsonObject[aNumber]==93.93, 1, urn:ngsi-ld:BeeHive:02",
        "jsonObject[anObject.name]==\"River\", 1, urn:ngsi-ld:BeeHive:02",
        "jsonObject[anObject.name]==\"Sea\", 0, ",
        "integer==143..213, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==144..213, 1, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==100..120, 0, ",
        "listOfString==\"iot\", 1, urn:ngsi-ld:BeeHive:01",
        "listOfString==\"stellio\", 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'listOfString==\"iot\",\"dataviz\"', 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'listOfString==\"fiware\",\"egm\"', 0, ",
        "'listOfInt==12,14', 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'listOfInt==12', 1, urn:ngsi-ld:BeeHive:01",
        "date, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'"
    )
    fun `it should retrieve entities according to q parameter`(
        q: String,
        expectedCount: Int,
        expectedListOfEntities: String?
    ) = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    q = q,
                    limit = 2,
                    offset = 0,
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(expectedCount, entitiesIds.size)
        if (expectedListOfEntities != null)
            assertThat(expectedListOfEntities.split(",")).containsAll(entitiesIds.toListOfString())
    }

    @ParameterizedTest
    @CsvSource(
        "near;minDistance==1600000, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, -1.0], [100.0, 0.0]]]', 0",
        "near;maxDistance==1000, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, -1.0], [100.0, 0.0]]]', 1",
        "contains, Polygon, '[[[90.0, 0.0], [100.0, 10.0], [110.0, 0.0], [100.0, -10.0], [90.0, 0.0]]]', 1",
        "contains, Polygon, '[[[80.0, 0.0], [90.0, 5.0], [90.0, 0.0], [80.0, 0.0]]]', 0",
        "equals, Point, '[100.0, 0.0]', 1",
        "equals, Point, '[101.0, 0.0]', 0",
        "intersects, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]', 1",
        "intersects, Polygon, '[[[101.0, 0.0], [102.0, 0.0], [102.0, -1.0], [101.0, 0.0]]]', 0",
        "disjoint, Point, '[101.0, 0.0]', 2",
        "disjoint, Polygon, '[[[100.0, 0.0], [101.0, 0.0], [101.0, -1.0], [100.0, 0.0]]]', 1"
    )
    fun `it should retrieve entities according to geoquery parameter`(
        georel: String,
        geometry: String,
        coordinates: String,
        expectedCount: Int
    ) = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    limit = 2,
                    offset = 0,
                    geoQuery = GeoQuery(
                        georel = georel,
                        geometry = GeoQuery.GeometryType.forType(geometry)!!,
                        coordinates = coordinates,
                        wktCoordinates = geoJsonToWkt(
                            GeoQuery.GeometryType.forType(geometry)!!,
                            coordinates
                        ).getOrNull()!!
                    ),
                    context = APIC_COMPOUND_CONTEXT
                )
            ) { null }

        assertEquals(expectedCount, entitiesIds.size)
    }

    @Test
    fun `it should retrieve entities according to access rights`() = runTest {
        val entitiesIds =
            entityPayloadService.queryEntities(
                QueryParams(
                    type = BEEHIVE_TYPE,
                    limit = 30,
                    offset = 0,
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
                    type = BEEHIVE_TYPE,
                    limit = 30,
                    offset = 0,
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
                    type = BEEHIVE_TYPE,
                    limit = 30,
                    offset = 0,
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
                type = BEEHIVE_TYPE,
                limit = 30,
                offset = 0,
                context = APIC_COMPOUND_CONTEXT
            )
        ) { null }.shouldSucceedWith { assertEquals(2, it) }
    }

    @Test
    fun `it should retrieve the count of entities according to access rights`() = runTest {
        entityPayloadService.queryEntitiesCount(
            QueryParams(
                ids = setOf(entity02Uri, entity01Uri),
                type = BEEHIVE_TYPE,
                limit = 30,
                offset = 0,
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
                    ids = setOf(entity02Uri, entity01Uri),
                    type = "https://ontology.eglobalmark.com/apic#UnknownType",
                    limit = 2,
                    offset = 10,
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
                    type = BEEHIVE_TYPE,
                    limit = 2,
                    offset = 10,
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
