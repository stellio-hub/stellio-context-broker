package com.egm.stellio.entity.repository

import com.egm.stellio.entity.config.ApplicationProperties
import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.toUri
import junit.framework.TestCase.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class)
@TestPropertySource(properties = ["application.authentication.enabled=false"])
@Import(TestContainersConfiguration::class)
class StandaloneNeo4jSearchRepositoryTests {

    @Autowired
    private lateinit var searchRepository: SearchRepository

    @Autowired
    private lateinit var standaloneNeo4jSearchRepository: StandaloneNeo4jSearchRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    private val beekeeperUri = "urn:ngsi-ld:Beekeeper:1230".toUri()
    private val deadFishUri = "urn:ngsi-ld:DeadFishes:019BN".toUri()
    private val partialTargetEntityUri = "urn:ngsi-ld:Entity:4567".toUri()
    private val userId = ""
    private val page = 1
    private val limit = 20

    @Test
    fun `it should return an entity if type and string properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "name==\"Scalpa\""),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if string properties are wrong`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "name==\"ScalpaXYZ\""),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and integer properties are correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "DeadFishes", "idPattern" to null, "q" to "fishNumber==500"),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if integer properties are wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishNumber", value = 500))
        )

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "DeadFishes", "idPattern" to null, "q" to "fishNumber==499"),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and float properties are correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 120.50))
        )

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "DeadFishes", "idPattern" to null, "q" to "fishWeight==120.50"),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if float properties are wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = -120.50))
        )

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "DeadFishes", "idPattern" to null, "q" to "fishWeight==-120"),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given weight equals to entity weight and comparison parameter is wrong`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 180.9))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "DeadFishes", "idPattern" to null, "q" to "fishWeight>180.9"),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given weight equals to entity weight and comparison parameter is correct`() {
        val entity = createEntity(
            deadFishUri,
            listOf("DeadFishes"),
            mutableListOf(Property(name = "fishWeight", value = 255))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "DeadFishes", "idPattern" to null, "q" to "fishWeight>=255"),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an empty list if given name equals to entity name and comparison parameter is wrong`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "ScalpaXYZ"))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "DeadFishes", "idPattern" to null, "q" to "name!=\"ScalpaXYZ\""),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if given name not equals to entity name and comparison parameter is correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "name!=\"ScalpaXYZ\""),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if observedAt of property is correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(
                    name = "testedAt",
                    value = "measure",
                    observedAt = ZonedDateTime.parse("2018-12-04T12:00:00Z")
                )
            )
        )
        val entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "testedAt.observedAt>2018-12-04T00:00:00Z;testedAt.observedAt<2018-12-04T18:00:00Z"
            ),
            userId,
            page,
            limit
        ).second
        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and dateTime properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = ZonedDateTime.parse("2018-12-04T12:00:00Z")))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "testedAt==2018-12-04T12:00:00Z"),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and date properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalDate.parse("2018-12-04")))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "testedAt==2018-12-04"),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not return an entity if type is correct but not the compared date`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalDate.parse("2018-12-04")))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "testedAt==2018-12-07"),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and time properties are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "testedAt", value = LocalTime.parse("12:00:00")))
        )
        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "testedAt==12:00:00"),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type, time and string are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "testedAt", value = LocalTime.parse("12:00:00")),
                Property(name = "name", value = "beekeeper")
            )
        )
        val entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "testedAt==12:00:00;name==\"beekeeper\""
            ),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should not return an entity if one of time or string is not correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "testedAt", value = LocalTime.parse("12:00:00")),
                Property(name = "name", value = "beekeeper")
            )
        )

        var entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "testedAt==13:00:00;name==\"beekeeper\""
            ),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "testedAt==12:00:00;name==\"beekeeperx\""
            ),
            userId,
            page,
            limit
        ).second
        assertFalse(entities.contains(entity.id))

        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return an entity if type and combinations of time, string and relationship are correct`() {
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(
                Property(name = "testedAt", value = LocalTime.parse("12:00:00")),
                Property(name = "name", value = "beekeeper")
            )
        )
        createRelationship(EntitySubjectNode(entity.id), "observedBy", partialTargetEntityUri)

        var entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\""
            ),
            userId,
            page,
            limit
        ).second
        assertTrue(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "(testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\");name==\"beekeeper\""
            ),
            userId,
            page,
            limit
        ).second
        assertTrue(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "(testedAt==12:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\")|name==\"beekeeper\""
            ),
            userId,
            page,
            limit
        ).second
        assertTrue(entities.contains(entity.id))

        entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to "(testedAt==13:00:00;observedBy==\"urn:ngsi-ld:Entity:4567\")|name==\"beekeeper\""
            ),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return entities matching given type and id`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val entities = searchRepository.getEntities(
            mapOf(
                "id" to listOf("urn:ngsi-ld:Beekeeper:1231"),
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to ""
            ),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should return entities matching given type and ids`() {
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )
        val entities = searchRepository.getEntities(
            mapOf(
                "id" to listOf("urn:ngsi-ld:Beekeeper:1231", "urn:ngsi-ld:Beekeeper:1232"),
                "type" to "Beekeeper",
                "idPattern" to null,
                "q" to ""
            ),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        assertTrue(entities.contains(thirdEntity.id))
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should return entities matching given type and idPattern`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:11232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )
        val entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to "^urn:ngsi-ld:Beekeeper:0.*2$",
                "q" to ""
            ),
            userId,
            page,
            limit
        ).second

        assertFalse(entities.contains(firstEntity.id))
        assertTrue(entities.contains(secondEntity.id))
        assertFalse(entities.contains(thirdEntity.id))
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should return no entity if given idPattern is not matching`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:11232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )
        val entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to "^urn:ngsi-ld:BeeHive:.*",
                "q" to ""
            ),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.isEmpty())
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should return matching entities count`() {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:03432".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )
        val entitiesCount = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to "^urn:ngsi-ld:Beekeeper:0.*2$",
                "q" to ""
            ),
            userId,
            page,
            limit
        ).first

        assertEquals(entitiesCount, 2)
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.entity.util.QueryEntitiesParameterizedTests#rawResultsProvider")
    fun `it should only return matching entities requested by pagination`(
        idPattern: String?,
        page: Int,
        limit: Int,
        expectedEntitiesIds: List<URI>
    ) {
        val firstEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:01232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa2"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:03432".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa3"))
        )

        val entities = searchRepository.getEntities(
            mapOf(
                "id" to null,
                "type" to "Beekeeper",
                "idPattern" to idPattern,
                "q" to ""
            ),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.containsAll(expectedEntitiesIds))
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    fun createEntity(
        id: URI,
        type: List<String>,
        properties: MutableList<Property> = mutableListOf(),
        location: String? = null
    ): Entity {
        val entity = Entity(id = id, type = type, properties = properties, location = location)
        return entityRepository.save(entity)
    }

    fun createRelationship(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        objectId: URI,
        datasetId: URI? = null
    ): Relationship {
        val relationship = Relationship(type = listOf(relationshipType), datasetId = datasetId)
        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
