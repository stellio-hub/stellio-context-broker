package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.authorization.service.AuthorizationService
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.EntitiesQueryFromPost
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.temporal.service.AttributeInstanceService
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.queryparameter.GeoQuery
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEKEEPER_IRI
import com.egm.stellio.shared.util.DEVICE_IRI
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.SENSOR_IRI
import com.egm.stellio.shared.util.geoJsonToWkt
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.sampleDataToNgsiLdEntity
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toListOfString
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
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
import org.springframework.data.r2dbc.core.delete
import org.springframework.data.relational.core.query.Criteria
import org.springframework.data.relational.core.query.Query
import org.springframework.data.relational.core.query.Update
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
class EntityServiceQueriesTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var entityQueryService: EntityQueryService

    @Autowired
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    @MockkBean(relaxed = true)
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    val entity01Uri = "urn:ngsi-ld:BeeHive:01".toUri()
    val entity02Uri = "urn:ngsi-ld:BeeHive:02".toUri()
    val entity04Uri = "urn:ngsi-ld:Beekeeper:04".toUri()
    val entity05Uri = "urn:ngsi-ld:Apiary:05".toUri()

    @BeforeAll
    fun createEntities() {
        val firstRawEntity = loadSampleData("entity_with_all_attributes_1.jsonld")
        val secondRawEntity = loadSampleData("entity_with_all_attributes_2.jsonld")
        val thirdRawEntity = loadSampleData("entity_with_multi_types.jsonld")
        val fourthRawEntity = loadSampleData("beekeeper.jsonld")
        val fifthRawEntity = loadSampleData("apiary.jsonld")

        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { attributeInstanceService.create(any()) } returns Unit.right()
        coEvery { authorizationService.createOwnerRight(any()) } returns Unit.right()

        runBlocking {
            listOf(firstRawEntity, secondRawEntity, thirdRawEntity, fourthRawEntity, fifthRawEntity).forEach {
                val (expandedEntity, ngsiLdEntity) = it.sampleDataToNgsiLdEntity().shouldSucceedAndResult()
                entityService.createEntity(ngsiLdEntity, expandedEntity).shouldSucceed()
            }
        }
    }

    @AfterAll
    fun deleteEntities() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
        r2dbcEntityTemplate.delete<Attribute>().from("temporal_entity_attribute").all().block()
    }

    @ParameterizedTest
    @CsvSource(
        "$BEEHIVE_IRI, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "'$APIARY_IRI|$BEEKEEPER_IRI', 3, 'urn:ngsi-ld:Apiary:05,urn:ngsi-ld:Beekeeper:04,urn:ngsi-ld:MultiTypes:03'",
        "'$APIARY_IRI,$BEEKEEPER_IRI', 3, 'urn:ngsi-ld:Apiary:05,urn:ngsi-ld:Beekeeper:04,urn:ngsi-ld:MultiTypes:03'",
        "$BEEKEEPER_IRI, 2, 'urn:ngsi-ld:Beekeeper:04,urn:ngsi-ld:MultiTypes:03'",
        "'$BEEKEEPER_IRI;$SENSOR_IRI', 1, urn:ngsi-ld:MultiTypes:03",
        "'$BEEKEEPER_IRI;$DEVICE_IRI', 0, ",
        "$DEVICE_IRI, 0, ",
        "$SENSOR_IRI, 1, urn:ngsi-ld:MultiTypes:03",
        "'($BEEKEEPER_IRI;$SENSOR_IRI),$DEVICE_IRI', 1, urn:ngsi-ld:MultiTypes:03"
    )
    fun `it should retrieve entities according types selection languages`(
        types: String,
        expectedCount: Int,
        expectedListOfEntities: String?
    ) = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = types,
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
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
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    scopeQ = scopeQ,
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(expectedCount, entitiesIds.size)
        if (expectedListOfEntities != null)
            assertThat(expectedListOfEntities.split(",")).containsAll(entitiesIds.toListOfString())
    }

    @Test
    fun `it should retrieve entities according to ids and a type`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    ids = setOf(entity02Uri),
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 2, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity02Uri)
    }

    @Test
    fun `it should retrieve entities according to ids and a selection of types`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    ids = setOf(entity02Uri, entity05Uri),
                    typeSelection = "$APIARY_IRI|$BEEHIVE_IRI",
                    paginationQuery = PaginationQuery(limit = 2, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(2, entitiesIds.size)
        assertThat(entitiesIds).contains(entity02Uri, entity05Uri)
    }

    @Test
    fun `it should retrieve entities according to attrs and types`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 2, offset = 0),
                    attrs = setOf(NAME_IRI),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri)
    }

    @Test
    fun `it should retrieve entities by a list of ids`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    ids = setOf(entity02Uri),
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 1, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity02Uri)
    }

    @Test
    fun `it should retrieve entities with respect to limit and offset`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 1, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
    }

    @Test
    fun `it should retrieve entities with respect to idPattern`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    idPattern = ".*urn:ngsi-ld:BeeHive:01.*",
                    paginationQuery = PaginationQuery(limit = 1, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(1, entitiesIds.size)
        assertThat(entitiesIds).contains(entity01Uri)
    }

    @ParameterizedTest
    @CsvSource(
        "integer==213, 1, urn:ngsi-ld:BeeHive:01",
        "relationship==urn:ngsi-ld:AnotherEntity:01, 1, urn:ngsi-ld:BeeHive:01",
        "relationship==\"urn:ngsi-ld:AnotherEntity:01\", 1, urn:ngsi-ld:BeeHive:01",
        "relationship==urn:ngsi-ld:YetAnotherEntity:01, 0, ",
        "relationship==\"urn:ngsi-ld:YetAnotherEntity:01\", 0, ",
        "relationship==urn:ngsi-ld:AnotherEntity:01;name==\"name\", 1, urn:ngsi-ld:BeeHive:01",
        "relationship==urn:ngsi-ld:AnotherEntity:01;name==\"anotherName\", 0, ",
        "(float==21.34|float==44.75), 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==213;boolean==true, 1, urn:ngsi-ld:BeeHive:01",
        "(integer>200|integer<100);observedProperty.observedAt<2023-02-25T00:00:00Z, 1, urn:ngsi-ld:BeeHive:01",
        "string~=\"(?i)another.*\", 1, urn:ngsi-ld:BeeHive:02",
        "string!~=\"(?i)another.*\", 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:Apiary:05'",
        "(string!~=\"(?i)another.*\";integer==213), 1, urn:ngsi-ld:BeeHive:01",
        "simpleQuoteString~=\"(?i).*It's a name.*\", 1, urn:ngsi-ld:BeeHive:01",
        "simpleQuoteString~=\"(?i)^it's.*\", 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "simpleQuoteString==\"It's a name\", 1, urn:ngsi-ld:BeeHive:01",
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
        "jsonObject[aSimpleQuote]==\"precipitation's measures\", 1, urn:ngsi-ld:BeeHive:01",
        "jsonObject[anObject.name]==\"River\", 1, urn:ngsi-ld:BeeHive:02",
        "jsonObject[anObject.name]==\"Sea\", 0, ",
        "integer==143..213, 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==144..213, 1, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        "integer==100..120, 0, ",
        "listOfString==\"iot\", 1, urn:ngsi-ld:BeeHive:01",
        "listOfString==\"data's processing\", 1, urn:ngsi-ld:BeeHive:01",
        "listOfString==\"stellio\", 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'",
        """'listOfString=="iot","dataviz"', 2, 'urn:ngsi-ld:BeeHive:01,urn:ngsi-ld:BeeHive:02'""",
        """'listOfString=="fiware","egm"', 0, """,
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
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    q = q,
                    paginationQuery = PaginationQuery(limit = 2, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
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
        "within, Polygon, '[[[90.0, 0.0], [100.0, 10.0], [110.0, 0.0], [100.0, -10.0], [90.0, 0.0]]]', 1",
        "within, Polygon, '[[[80.0, 0.0], [90.0, 5.0], [90.0, 0.0], [80.0, 0.0]]]', 0",
        "contains, Polygon, '[[[90.0, 0.0], [100.0, 10.0], [110.0, 0.0], [100.0, -10.0], [90.0, 0.0]]]', 0",
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
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 2, offset = 0),
                    geoQuery = GeoQuery(
                        georel = georel,
                        geometry = GeoQuery.GeometryType.forType(geometry)!!,
                        coordinates = coordinates,
                        wktCoordinates = geoJsonToWkt(
                            GeoQuery.GeometryType.forType(geometry)!!,
                            coordinates
                        ).getOrNull()!!
                    ),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertEquals(expectedCount, entitiesIds.size)
    }

    @Test
    fun `it should retrieve entities according to one entity selector`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromPost(
                    entitySelectors = listOf(
                        EntitySelector(id = null, idPattern = null, typeSelection = BEEHIVE_IRI)
                    ),
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertThat(entitiesIds)
            .hasSize(2)
            .contains(entity01Uri, entity02Uri)
    }

    @Test
    fun `it should retrieve entities with a query without an entity selector`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromPost(
                    scopeQ = "/Madrid/Gardens/ParqueNorte",
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertThat(entitiesIds)
            .hasSize(2)
            .contains(entity01Uri, entity02Uri)
    }

    @Test
    fun `it should retrieve entities according to two entity selectors with type and idPattern`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromPost(
                    entitySelectors = listOf(
                        EntitySelector(id = null, idPattern = null, typeSelection = BEEHIVE_IRI),
                        EntitySelector(
                            id = null,
                            idPattern = "urn:ngsi-ld:Beekeeper:*",
                            typeSelection = BEEKEEPER_IRI
                        )
                    ),
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertThat(entitiesIds)
            .hasSize(3)
            .contains(entity01Uri, entity02Uri, entity04Uri)
    }

    @Test
    fun `it should retrieve entities according to two entity selectors with type and id`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromPost(
                    entitySelectors = listOf(
                        EntitySelector(id = null, idPattern = null, typeSelection = BEEHIVE_IRI),
                        EntitySelector(
                            id = entity05Uri,
                            idPattern = null,
                            typeSelection = APIARY_IRI
                        )
                    ),
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertThat(entitiesIds)
            .hasSize(3)
            .contains(entity01Uri, entity02Uri, entity05Uri)
    }

    @Test
    fun `it should retrieve entities according to two entity selectors with one not matching any entities`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromPost(
                    entitySelectors = listOf(
                        EntitySelector(id = null, idPattern = null, typeSelection = BEEHIVE_IRI),
                        EntitySelector(
                            id = entity05Uri,
                            idPattern = null,
                            typeSelection = BEEKEEPER_IRI
                        )
                    ),
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertThat(entitiesIds)
            .hasSize(2)
            .contains(entity01Uri, entity02Uri)
    }

    @Test
    fun `it should retrieve entities according to access rights`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
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
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
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
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 30, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
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
        entityQueryService.queryEntitiesCount(
            EntitiesQueryFromGet(
                typeSelection = BEEHIVE_IRI,
                paginationQuery = PaginationQuery(limit = 30, offset = 0),
                contexts = APIC_COMPOUND_CONTEXTS
            )
        ) { null }.shouldSucceedWith { assertEquals(2, it) }
    }

    @Test
    fun `it should retrieve the count of entities according to access rights`() = runTest {
        entityQueryService.queryEntitiesCount(
            EntitiesQueryFromGet(
                ids = setOf(entity02Uri, entity01Uri),
                typeSelection = BEEHIVE_IRI,
                paginationQuery = PaginationQuery(limit = 30, offset = 0),
                contexts = APIC_COMPOUND_CONTEXTS
            )
        ) { "entity_payload.entity_id IN ('urn:ngsi-ld:BeeHive:01')" }
            .shouldSucceedWith { assertEquals(1, it) }
    }

    @Test
    fun `it should return an empty list if no entity matches the requested type`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    ids = setOf(entity02Uri, entity01Uri),
                    typeSelection = "https://ontology.eglobalmark.com/apic#UnknownType",
                    paginationQuery = PaginationQuery(limit = 2, offset = 10),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertThat(entitiesIds).isEmpty()
    }

    @Test
    fun `it should return an empty list if no entity matches the requested attributes`() = runTest {
        val entitiesIds =
            entityQueryService.queryEntities(
                EntitiesQueryFromGet(
                    typeSelection = BEEHIVE_IRI,
                    paginationQuery = PaginationQuery(limit = 2, offset = 10),
                    attrs = setOf("unknownAttribute"),
                    contexts = APIC_COMPOUND_CONTEXTS
                )
            ) { null }

        assertThat(entitiesIds).isEmpty()
    }

    private fun updateSpecificAccessPolicy(
        entityId: URI,
        specificAccessPolicy: AuthContextModel.SpecificAccessPolicy
    ) = r2dbcEntityTemplate.update(Entity::class.java).inTable("entity_payload")
        .matching(Query.query(Criteria.where("entity_id").`is`(entityId)))
        .apply(Update.update("specific_access_policy", specificAccessPolicy.toString())).block()

    private fun resetSpecificAccessPolicy(
        entityId: URI
    ) = r2dbcEntityTemplate.update(Entity::class.java).inTable("entity_payload")
        .matching(Query.query(Criteria.where("entity_id").`is`(entityId)))
        .apply(Update.update("specific_access_policy", null)).block()
}
