package com.egm.stellio.entity.repository

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.EGM_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.SERVICE_ACCOUNT_ID
import com.egm.stellio.entity.authorization.Neo4jAuthorizationRepositoryTest.Companion.EGM_CAN_ADMIN
import com.egm.stellio.entity.authorization.Neo4jAuthorizationRepositoryTest.Companion.EGM_CAN_READ
import com.egm.stellio.entity.authorization.Neo4jAuthorizationRepositoryTest.Companion.EGM_CAN_WRITE
import com.egm.stellio.entity.authorization.Neo4jAuthorizationRepositoryTest.Companion.EGM_IS_MEMBER_OF
import com.egm.stellio.entity.authorization.Neo4jAuthorizationService
import com.egm.stellio.entity.config.ApplicationProperties
import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
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

@SpringBootTest
@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class)
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

    @MockkBean(relaxed = true)
    private lateinit var neo4jAuthorizationService: Neo4jAuthorizationService

    private val beekeeperUri = "urn:ngsi-ld:Beekeeper:1230".toUri()
    private val userId = "urn:ngsi-ld:User:01"
    private val clientId = "urn:ngsi-ld:Client:01"
    private val serviceAccountId = "urn:ngsi-ld:ServiceAccount:01"
    private val groupId = "urn:ngsi-ld:Group:01"
    private val page = 1
    private val limit = 20

    @Test
    fun `it should return matching entities that user can access`() {
        val userEntity = createEntity(userId.toUri(), listOf("User"), mutableListOf())
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_ADMIN, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, thirdEntity.id)

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "name==\"Scalpa\""),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id, thirdEntity.id)))
        neo4jRepository.deleteEntity(userEntity.id)
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
        neo4jRepository.deleteEntity(thirdEntity.id)
    }

    @Test
    fun `it should return matching entities that user can access by it's group`() {
        val userEntity = createEntity(userId.toUri(), listOf("User"), mutableListOf())
        val groupEntity = createEntity(groupId.toUri(), listOf("Group"), mutableListOf())
        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(groupEntity.id), EGM_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(groupEntity.id), EGM_CAN_WRITE, secondEntity.id)

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "name==\"Scalpa\""),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
        neo4jRepository.deleteEntity(userEntity.id)
        neo4jRepository.deleteEntity(groupEntity.id)
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should not return a matching entity that user cannot access`() {
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
        neo4jRepository.deleteEntity(userId.toUri())
        neo4jRepository.deleteEntity(entity.id)
    }

    @Test
    fun `it should return matching entities that client can access`() {
        val clientEntity = createEntity(
            clientId.toUri(),
            listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = serviceAccountId.toUri()
                )
            )
        )
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_READ, firstEntity.id)
        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_READ, secondEntity.id)

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "name==\"Scalpa\""),
            serviceAccountId,
            page,
            limit
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
        neo4jRepository.deleteEntity(clientEntity.id)
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should return all matching entities for admin users`() {
        val userEntity = createEntity(
            userId.toUri(),
            listOf("User"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )
        val firstEntity = createEntity(
            beekeeperUri,
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )
        val secondEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1231".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = "name", value = "Scalpa"))
        )

        every { neo4jAuthorizationService.userIsAdmin(any()) } returns true

        val entities = searchRepository.getEntities(
            mapOf("id" to null, "type" to "Beekeeper", "idPattern" to null, "q" to "name==\"Scalpa\""),
            userId,
            page,
            limit
        ).second

        assertTrue(entities.containsAll(listOf(firstEntity.id, secondEntity.id)))
        neo4jRepository.deleteEntity(userEntity.id)
        neo4jRepository.deleteEntity(firstEntity.id)
        neo4jRepository.deleteEntity(secondEntity.id)
    }

    @Test
    fun `it should return matching entities count`() {
        val userEntity = createEntity(userId.toUri(), listOf("User"), mutableListOf())
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
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, thirdEntity.id)

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
        neo4jRepository.deleteEntity(userEntity.id)
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
        val userEntity = createEntity(userId.toUri(), listOf("User"), mutableListOf())
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
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, thirdEntity.id)

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
        neo4jRepository.deleteEntity(userEntity.id)
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
