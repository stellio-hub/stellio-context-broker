package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.EGM_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.SERVICE_ACCOUNT_ID
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
import java.net.URI

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
    fun `it should filter entities authorized for user with given rights`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01", listOf("User"), mutableListOf())
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01", listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:01",
                listOf("urn:ngsi-ld:Apiary:01"),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(availableRightsForEntities == listOf("urn:ngsi-ld:Apiary:01"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
    }

    @Test
    fun `it should find no entities are authorized by user`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01", listOf("User"), mutableListOf())
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01", listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:01",
                listOf("urn:ngsi-ld:Apiary:01"),
                setOf(EGM_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
    }

    @Test
    fun `it should filter entities that are authorized by user's group`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01", listOf("User"), mutableListOf())
        val groupEntity = createEntity("urn:ngsi-ld:Group:01", listOf("Group"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01", listOf("Apiary"), mutableListOf())
        val apiaryEntity2 = createEntity("urn:ngsi-ld:Apiary:02", listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, apiaryEntity.id)
        createRelationship(EntitySubjectNode(groupEntity.id), EGM_CAN_READ, apiaryEntity2.id)

        val authorizedEntitiesId =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:01",
                listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02"),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(authorizedEntitiesId == listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:02")
    }

    @Test
    fun `it should find no entities are authorized for client`() {
        val clientEntity = createEntity(
            "urn:ngsi-ld:Client:01", listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2"
                )
            )
        )
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01", listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2",
                listOf("urn:ngsi-ld:Apiary:01"),
                setOf(EGM_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
    }

    @Test
    fun `it should filter entities authorized for client with given rights`() {
        val clientEntity = createEntity(
            "urn:ngsi-ld:Client:01", listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2"
                )
            )
        )
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01", listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2",
                listOf("urn:ngsi-ld:Apiary:01"),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(availableRightsForEntities == listOf("urn:ngsi-ld:Apiary:01"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
    }

    @Test
    fun `it should get all user's roles`() {
        createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin", "creator")
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles == setOf("admin", "creator"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
    }

    @Test
    fun `it should get all client's roles`() {
        createEntity(
            "urn:ngsi-ld:Client:01",
            listOf("Client"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin", "creator")
                ),
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "some-uuid"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("some-uuid")

        assert(roles == setOf("admin", "creator"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01")
    }

    @Test
    fun `it should get a user's single role`() {
        createEntity(
            "urn:ngsi-ld:User:01",
            listOf("User"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
    }

    @Test
    fun `it should get all user's roles from group`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01", listOf("User"), mutableListOf())

        val groupEntity = createEntity(
            "urn:ngsi-ld:Group:01",
            listOf("Group"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin")
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Group:01")
    }

    @Test
    fun `it should get a user's single role from group`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01", listOf("User"), mutableListOf())

        val groupEntity = createEntity(
            "urn:ngsi-ld:Group:01",
            listOf("Group"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Group:01")
    }

    @Test
    fun `it should find no user roles`() {
        createEntity("urn:ngsi-ld:User:01", listOf("User"), mutableListOf())

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01")

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
    }

    @Test
    fun `it should find no client roles`() {
        createEntity(
            "urn:ngsi-ld:Client:01",
            listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "some-uuid"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("some-uuid")

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01")
    }

    @Test
    fun `it should find no client roles if the service account is not known`() {
        createEntity(
            "urn:ngsi-ld:Client:01",
            listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "some-uuid"
                ),
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin", "creator")
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("unknown-uuid")

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01")
    }

    @Test
    fun `it should create admin links to entities`() {
        createEntity("urn:ngsi-ld:User:01", listOf("User"), mutableListOf())
        createEntity("urn:ngsi-ld:Apiary:01", listOf("Apiary"), mutableListOf())
        createEntity("urn:ngsi-ld:Apiary:02", listOf("Apiary"), mutableListOf())

        val targetIds = listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02")

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            "urn:ngsi-ld:User:01",
            targetIds.map {
                Relationship(
                    type = listOf(R_CAN_ADMIN),
                    datasetId = URI.create("urn:ngsi-ld:Dataset:rCanAdmin:$it")
                )
            },
            targetIds
        )

        assert(createdRelations.size == 2)

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:02")
    }

    @Test
    fun `it should create admin links to entities for a client`() {
        createEntity(
            "urn:ngsi-ld:Client:01", listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2"
                )
            )
        )
        createEntity("urn:ngsi-ld:Apiary:01", listOf("Apiary"), mutableListOf())
        createEntity("urn:ngsi-ld:Apiary:02", listOf("Apiary"), mutableListOf())

        val targetIds = listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02")

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2",
            targetIds.map {
                Relationship(
                    type = listOf(R_CAN_ADMIN),
                    datasetId = URI.create("urn:ngsi-ld:Dataset:rCanAdmin:$it")
                )
            },
            targetIds
        )

        assert(createdRelations.size == 2)

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01")
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:02")
    }

    fun createEntity(id: String, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        return entityRepository.save(entity)
    }

    fun createRelationship(subjectNodeInfo: SubjectNodeInfo, relationshipType: String, objectId: String): Relationship {
        val relationship = Relationship(type = listOf(relationshipType))

        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
