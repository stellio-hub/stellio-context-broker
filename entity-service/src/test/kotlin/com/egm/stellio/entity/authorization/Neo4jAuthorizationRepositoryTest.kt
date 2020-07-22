package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.config.TestContainersConfiguration
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.SubjectNodeInfo
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
@Import(TestContainersConfiguration::class)
class Neo4jAuthorizationRepositoryTest {

    @Autowired
    private lateinit var neo4jAuthorizationRepository: Neo4jAuthorizationRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    companion object {
        const val EGM_CAN_READ = "https://ontology.eglobalmark.com/authorization#rCanRead"
        const val EGM_CAN_WRITE = "https://ontology.eglobalmark.com/authorization#rCanWrite"
        const val EGM_IS_MEMBER_OF = "https://ontology.eglobalmark.com/authorization#isMemberOf"
    }

    @Test
    fun `it should get all user's rights on an entity`() {
        val userEntity = createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf()
        )

        val apiaryEntity = createEntity(
            "urn:ngsi-ld:Apiary:01",
            listOf("Apiary"),
            mutableListOf()
        )

        createRelationship(
            EntitySubjectNode(userEntity.id),
            EGM_CAN_READ,
            apiaryEntity.id
        )

        val availableRightsForEntities =
            neo4jAuthorizationRepository.getAvailableRightsForEntities(
                "urn:ngsi-ld:User:01",
                listOf("urn:ngsi-ld:Apiary:01")
            )

        assert(availableRightsForEntities.size == 1)
        assert(availableRightsForEntities[0].targetEntityId == "urn:ngsi-ld:Apiary:01")
        assert(availableRightsForEntities[0].right?.type == listOf(EGM_CAN_READ))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
    }

    @Test
    fun `it should get all user's rights on an entity from group`() {
        val userEntity = createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf()
        )

        val groupEntity = createEntity(
            "urn:ngsi-ld:Group:01",
            listOf("Group"),
            mutableListOf(
                Property(
                    name = "https://ontology.eglobalmark.com/authorization#roles",
                    value = listOf("admin")
                )
            )
        )

        createRelationship(
            EntitySubjectNode(userEntity.id),
            EGM_IS_MEMBER_OF,
            groupEntity.id
        )

        val apiaryEntity = createEntity(
            "urn:ngsi-ld:Apiary:01",
            listOf("Apiary"),
            mutableListOf()
        )

        createRelationship(
            EntitySubjectNode(groupEntity.id),
            EGM_CAN_READ,
            apiaryEntity.id
        )

        createRelationship(
            EntitySubjectNode(userEntity.id),
            EGM_CAN_WRITE,
            apiaryEntity.id
        )

        val availableRightsForEntities =
            neo4jAuthorizationRepository.getAvailableRightsForEntities(
                "urn:ngsi-ld:User:01",
                listOf("urn:ngsi-ld:Apiary:01")
            )

        assert(availableRightsForEntities.size == 1)
        assert(availableRightsForEntities[0].targetEntityId == "urn:ngsi-ld:Apiary:01")
        assert(availableRightsForEntities[0].grpRight?.type == listOf(EGM_CAN_READ))
        assert(availableRightsForEntities[0].right?.type == listOf(EGM_CAN_WRITE))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Group:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
    }

    @Test
    fun `it should get all user's rights on several entities`() {
        val userEntity = createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf()
        )

        val apiaryEntity = createEntity(
            "urn:ngsi-ld:Apiary:01",
            listOf("Apiary"),
            mutableListOf()
        )

        val apiaryEntity2 = createEntity(
            "urn:ngsi-ld:Apiary:02",
            listOf("Apiary"),
            mutableListOf()
        )

        createRelationship(
            EntitySubjectNode(userEntity.id),
            EGM_CAN_WRITE,
            apiaryEntity.id
        )

        createRelationship(
            EntitySubjectNode(userEntity.id),
            EGM_CAN_READ,
            apiaryEntity2.id
        )

        val availableRightsForEntities =
            neo4jAuthorizationRepository.getAvailableRightsForEntities(
                "urn:ngsi-ld:User:01",
                listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02")
            )

        assert(availableRightsForEntities.size == 2)
        assert(
            availableRightsForEntities.find { it.targetEntityId == apiaryEntity.id }?.right?.type == listOf(
                EGM_CAN_WRITE
            )
        )
        assert(
            availableRightsForEntities.find { it.targetEntityId == apiaryEntity2.id }?.right?.type == listOf(
                EGM_CAN_READ
            )
        )

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:02")
    }

    @Test
    fun `it should get all user's roles`() {
        createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf(
                Property(
                    name = "https://ontology.eglobalmark.com/authorization#roles",
                    value = listOf("admin", "creator")
                )
            )
        )

        val roles =
            neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles == listOf("admin", "creator"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
    }

    @Test
    fun `it should get all user's roles from group`() {
        val userEntity = createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf()
        )

        val groupEntity = createEntity(
            "urn:ngsi-ld:Group:01",
            listOf("Group"),
            mutableListOf(
                Property(
                    name = "https://ontology.eglobalmark.com/authorization#roles",
                    value = listOf("admin")
                )
            )
        )

        createRelationship(
            EntitySubjectNode(userEntity.id),
            EGM_IS_MEMBER_OF,
            groupEntity.id
        )

        val roles =
            neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles == listOf("admin"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Group:01")
    }

    @Test
    fun `it should find no user roles`() {
        val userEntity = createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf()
        )

        val roles =
            neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
    }

    fun createEntity(id: String, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        return entityRepository.save(entity)
    }

    fun createRelationship(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        objectId: String
    ): Relationship {
        val relationship = Relationship(type = listOf(relationshipType))

        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
