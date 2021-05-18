package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.Neo4jAuthorizationRepositoryTest.Companion.EGM_CAN_READ
import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.toUri
import junit.framework.TestCase.assertFalse
import junit.framework.TestCase.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=true"])
@Import(TestContainersConfiguration::class)
class Neo4jSearchRepositoryTests {

    @Autowired
    private lateinit var searchRepository: SearchRepository

    @Autowired
    private lateinit var neo4jSearchRepository: Neo4jSearchRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    private val beekeeperUri = "urn:ngsi-ld:Beekeeper:1230".toUri()
    private val userId = "urn:ngsi-ld:User:01"
    private val page = 1
    private val limit = 20

    @Test
    fun `it should return a matching entity if user has read access on it`() {
        val userEntity = createEntity(userId.toUri(), listOf("User"), mutableListOf())
        val entity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, entity.id)

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
    fun `it should not return a matching entity if user has not access on it`() {
        createEntity(userId.toUri(), listOf("User"), mutableListOf())
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
        assertFalse(entities.contains(entity.id))
        neo4jRepository.deleteEntity(entity.id)
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
