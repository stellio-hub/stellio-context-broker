package com.egm.stellio.entity.repository

import arrow.core.None
import com.egm.stellio.entity.config.WithNeo4jContainer
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.net.URI
import java.time.*

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class StandaloneNeo4jSearchRepositoryTests : WithNeo4jContainer {

    @Autowired
    private lateinit var searchRepository: SearchRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    private val beekeeperUri = "urn:ngsi-ld:Beekeeper:1230".toUri()
    private val deadFishUri = "urn:ngsi-ld:DeadFishes:019BN".toUri()
    private val partialTargetEntityUri = "urn:ngsi-ld:Entity:4567".toUri()
    private val expandedNameProperty = expandJsonLdTerm("name", DEFAULT_CONTEXTS)
    private val sub = None
    private val offset = 0
    private val limit = 20

    @AfterEach
    fun cleanData() {
        entityRepository.deleteAll()
    }

    @Test
    fun `it should return an entity if type and string properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "name==\"Scalpa\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an empty list if string properties are wrong`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "name==\"ScalpaXYZ\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if type and integer properties are correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = expandJsonLdTerm("fishNumber", DEFAULT_CONTEXTS), value = 500))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("DeadFishes"), q = "fishNumber==500", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an empty list if integer properties are wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = expandJsonLdTerm("fishNumber", DEFAULT_CONTEXTS), value = 500))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("DeadFishes"), q = "fishNumber==499", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if type and float properties are correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = expandJsonLdTerm("fishWeight", DEFAULT_CONTEXTS), value = 120.50))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("DeadFishes"), q = "fishWeight==120.50", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an empty list if float properties are wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = expandJsonLdTerm("fishWeight", DEFAULT_CONTEXTS), value = -120.50))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("DeadFishes"), q = "fishWeight==-120", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return an empty list if given weight equals to entity weight and comparison parameter is wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = expandJsonLdTerm("fishWeight", DEFAULT_CONTEXTS), value = 180.9))
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("DeadFishes"), q = "fishWeight>180.9", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if given weight equals to entity weight and comparison parameter is correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = expandJsonLdTerm("fishWeight", DEFAULT_CONTEXTS), value = 255))
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("DeadFishes"), q = "fishWeight>=255", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an empty list if given name equals to entity name and comparison parameter is wrong`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "ScalpaXYZ"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("DeadFishes"), q = "name!=\"ScalpaXYZ\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if given name not equals to entity name and comparison parameter is correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "name!=\"ScalpaXYZ\"", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if observedAt of property is correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = "measure",
                    observedAt = ZonedDateTime.parse("2018-12-04T12:00:00Z")
                )
            )
        )
        val entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "testedAt.observedAt>2018-12-04T00:00:00Z;testedAt.observedAt<2018-12-04T18:00:00Z",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second
        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if type and dateTime properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = ZonedDateTime.parse("2018-12-04T12:00:00Z")
                )
            )
        )
        val entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "testedAt==2018-12-04T12:00:00Z",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if type and date properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = LocalDate.parse("2018-12-04")
                )
            )
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "testedAt==2018-12-04", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should not return an entity if type is correct but not the compared date`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = LocalDate.parse("2018-12-04")
                )
            )
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "testedAt==2018-12-07", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if type and time properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = LocalTime.parse("12:00:00")
                )
            )
        )
        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), q = "testedAt==12:00:00", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if type, time and string are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = LocalTime.parse("12:00:00")
                ),
                Property(name = expandedNameProperty, value = "beekeeper")
            )
        )
        val entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "testedAt==12:00:00;name==\"beekeeper\"",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should not return an entity if one of time or string is not correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = LocalTime.parse("12:00:00")
                ),
                Property(name = expandedNameProperty, value = "beekeeper")
            )
        )

        var entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "testedAt==13:00:00;name==\"beekeeper\"",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "testedAt==12:00:00;name==\"beekeeperx\"",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second
        assertFalse(entities.contains(entity.id))
    }

    @Test
    fun `it should return an entity if type and combinations of time, string and relationship are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = expandJsonLdTerm("testedAt", DEFAULT_CONTEXTS),
                    value = LocalTime.parse("12:00:00")
                ),
                Property(name = expandedNameProperty, value = "beekeeper")
            )
        )
        createRelationship(EntitySubjectNode(entity.id), "observedBy", partialTargetEntityUri)

        var entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\"",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second
        assertTrue(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "(testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\");name==\"beekeeper\"",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second
        assertTrue(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "(testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\")|name==\"beekeeper\"",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second
        assertTrue(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                q = "(testedAt==13:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\")|name==\"beekeeper\"",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.contains(entity.id))
    }

    @Test
    fun `it should return entities matching given type and id`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(
                ids = setOf("urn:ngsi-ld:Beekeeper:1231".toUri()),
                types = setOf("Beekeeper"),
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
    }

    @Test
    fun `it should return an entity matching given id without specifying a type`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper")
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper")
        )
        val entities = searchRepository.getEntities(
            QueryParams(ids = setOf("urn:ngsi-ld:Beekeeper:1231".toUri()), offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should return an entity matching creation date and an id (without specifying a type)`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper")
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper")
        )
        val entitiesCount = searchRepository.getEntities(
            QueryParams(
                ids = setOf("urn:ngsi-ld:Beekeeper:1231".toUri()),
                q = "createdAt>2021-07-10T00:00:00Z",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).first

        assertEquals(1, entitiesCount)

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should return entities matching creation date only (without specifying a type)`() {
        val now = Instant.now().atOffset(ZoneOffset.UTC)
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper")
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper")
        )
        val entitiesCount = searchRepository.getEntities(
            QueryParams(q = "createdAt>$now", offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).first

        assertEquals(2, entitiesCount)

        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should return entities matching given type and ids`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa3"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(
                ids = listOf("urn:ngsi-ld:Beekeeper:1231", "urn:ngsi-ld:Beekeeper:1232").toListOfUri().toSet(),
                types = setOf("Beekeeper"),
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        assertTrue(entities.contains(thirdEntity.id))
    }

    @Test
    fun `it should return entities matching given type and idPattern`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:11232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa3"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                idPattern = "^urn:ngsi-ld:Beekeeper:0.*2$",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        assertFalse(entities.contains(thirdEntity.id))
    }

    @Test
    fun `it should return no entity if given idPattern is not matching`() {
        createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        createEntity(
            "urn:ngsi-ld:Beekeeper:11232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa3"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                idPattern = "^urn:ngsi-ld:BeeHive:*",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.isEmpty())
    }

    @Test
    fun `it should return an entity matching attrs on a property`() {
        createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandJsonLdTerm("description", DEFAULT_CONTEXTS), value = "Scalpa2"))
        )
        val entities = searchRepository.getEntities(
            QueryParams(attrs = setOf(expandedNameProperty), offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertEquals(1, entities.size)
    }

    @Test
    fun `it should return an entity matching attrs on a relationship`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(entity.id), "observedBy", partialTargetEntityUri)

        val entities = searchRepository.getEntities(
            QueryParams(attrs = setOf("observedBy"), offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertEquals(1, entities.size)
    }

    @Test
    fun `it should return an entity matching attrs on a relationship and a property value`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(entity.id), "observedBy", partialTargetEntityUri)

        val entities = searchRepository.getEntities(
            QueryParams(q = "name==\"Scalpa\"", attrs = setOf("observedBy"), offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertEquals(1, entities.size)
    }

    @Test
    fun `it should return an entity matching attrs on a relationship and an entity type`() {
        val entity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(entity.id), "observedBy", partialTargetEntityUri)

        val entities = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                attrs = setOf("observedBy"),
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertEquals(1, entities.size)
    }

    @Test
    fun `it should return matching entities count`() {
        createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        createEntity(
            "urn:ngsi-ld:Beekeeper:03432".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa3"))
        )
        val entitiesCount = searchRepository.getEntities(
            QueryParams(
                types = setOf("Beekeeper"),
                idPattern = "^urn:ngsi-ld:Beekeeper:0.*2$",
                offset = offset,
                limit = limit
            ),
            sub,
            DEFAULT_CONTEXTS
        ).first

        assertEquals(entitiesCount, 2)
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.entity.util.QueryEntitiesParameterizedTests#rawResultsProvider")
    fun `it should only return matching entities requested by pagination`(
        idPattern: String?,
        offset: Int,
        limit: Int,
        expectedEntitiesIds: List<URI>
    ) {
        createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa"))
        )
        createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa2"))
        )
        createEntity(
            "urn:ngsi-ld:Beekeeper:03432".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = expandedNameProperty, value = "Scalpa3"))
        )

        val entities = searchRepository.getEntities(
            QueryParams(types = setOf("Beekeeper"), idPattern = idPattern, offset = offset, limit = limit),
            sub,
            DEFAULT_CONTEXTS
        ).second

        assertTrue(entities.containsAll(expectedEntitiesIds))
    }

    fun createEntity(
        id: URI,
        type: List<String>,
        properties: MutableList<Property> = mutableListOf(),
        location: String? = null
    ): Entity {
        val entity = Entity(id = id, types = type, properties = properties, location = location)
        return entityRepository.save(entity)
    }

    fun createRelationship(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        objectId: URI,
        datasetId: URI? = null
    ): Relationship {
        val relationship = Relationship(objectId = objectId, type = listOf(relationshipType), datasetId = datasetId)
        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
