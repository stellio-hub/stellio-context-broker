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
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toUri
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
        val userEntity = createEntity("urn:ngsi-ld:User:01".toUri(), listOf("User"), mutableListOf())
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01".toUri(), listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:01".toUri(),
                listOf("urn:ngsi-ld:Apiary:01").toListOfUri(),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(availableRightsForEntities == listOf("urn:ngsi-ld:Apiary:01").toListOfUri())

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01".toUri())
    }

    @Test
    fun `it should find no entities are authorized by user`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01".toUri(), listOf("User"), mutableListOf())
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01".toUri(), listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:01".toUri(),
                listOf("urn:ngsi-ld:Apiary:01").toListOfUri(),
                setOf(EGM_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01".toUri())
    }

    @Test
    fun `it should filter entities that are authorized by user's group`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01".toUri(), listOf("User"), mutableListOf())
        val groupEntity = createEntity("urn:ngsi-ld:Group:01".toUri(), listOf("Group"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01".toUri(), listOf("Apiary"), mutableListOf())
        val apiaryEntity2 = createEntity("urn:ngsi-ld:Apiary:02".toUri(), listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, apiaryEntity.id)
        createRelationship(EntitySubjectNode(groupEntity.id), EGM_CAN_READ, apiaryEntity2.id)

        val authorizedEntitiesId =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:01".toUri(),
                listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02").toListOfUri(),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(authorizedEntitiesId == listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02").toListOfUri())

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:02".toUri())
    }

    @Test
    fun `it should find no entities are authorized for client`() {
        val clientEntity = createEntity(
            "urn:ngsi-ld:Client:01".toUri(), listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2"
                )
            )
        )
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01".toUri(), listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2".toUri(),
                listOf("urn:ngsi-ld:Apiary:01").toListOfUri(),
                setOf(EGM_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01".toUri())
    }

    @Test
    fun `it should filter entities authorized for client with given rights`() {
        val clientEntity = createEntity(
            "urn:ngsi-ld:Client:01".toUri(), listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2"
                )
            )
        )
        val apiaryEntity = createEntity("urn:ngsi-ld:Apiary:01".toUri(), listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2".toUri(),
                listOf("urn:ngsi-ld:Apiary:01").toListOfUri(),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(availableRightsForEntities == listOf("urn:ngsi-ld:Apiary:01").toListOfUri())

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01".toUri())
    }

    @Test
    fun `it should get all user's roles`() {
        createEntity(
            "urn:ngsi-ld:User:01".toUri(),
            listOf("User"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin", "creator")
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01".toUri())

        assert(roles == setOf("admin", "creator"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
    }

    @Test
    fun `it should get all client's roles`() {
        createEntity(
            "urn:ngsi-ld:Client:01".toUri(),
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

        val roles = neo4jAuthorizationRepository.getUserRoles("some-uuid".toUri())

        assert(roles == setOf("admin", "creator"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01".toUri())
    }

    @Test
    fun `it should get a user's single role`() {
        createEntity(
            "urn:ngsi-ld:User:01".toUri(),
            listOf("User"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01".toUri())

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
    }

    @Test
    fun `it should get all user's roles from group`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01".toUri(), listOf("User"), mutableListOf())

        val groupEntity = createEntity(
            "urn:ngsi-ld:Group:01".toUri(),
            listOf("Group"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin")
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01".toUri())

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Group:01".toUri())
    }

    @Test
    fun `it should get a user's single role from group`() {
        val userEntity = createEntity("urn:ngsi-ld:User:01".toUri(), listOf("User"), mutableListOf())

        val groupEntity = createEntity(
            "urn:ngsi-ld:Group:01".toUri(),
            listOf("Group"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01".toUri())

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Group:01".toUri())
    }

    @Test
    fun `it should find no user roles`() {
        createEntity("urn:ngsi-ld:User:01".toUri(), listOf("User"), mutableListOf())

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:01".toUri())

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
    }

    @Test
    fun `it should find no client roles`() {
        createEntity(
            "urn:ngsi-ld:Client:01".toUri(),
            listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "some-uuid"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("some-uuid".toUri())

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01".toUri())
    }

    @Test
    fun `it should find no client roles if the service account is not known`() {
        createEntity(
            "urn:ngsi-ld:Client:01".toUri(),
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

        val roles = neo4jAuthorizationRepository.getUserRoles("unknown-uuid".toUri())

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01".toUri())
    }

    @Test
    fun `it should create admin links to entities`() {
        createEntity("urn:ngsi-ld:User:01".toUri(), listOf("User"), mutableListOf())
        createEntity("urn:ngsi-ld:Apiary:01".toUri(), listOf("Apiary"), mutableListOf())
        createEntity("urn:ngsi-ld:Apiary:02".toUri(), listOf("Apiary"), mutableListOf())

        val targetIds = listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02").toListOfUri()

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            "urn:ngsi-ld:User:01".toUri(),
            targetIds.map {
                Relationship(
                    type = listOf(R_CAN_ADMIN),
                    datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
                )
            },
            targetIds
        )

        assert(createdRelations.size == 2)

        neo4jRepository.deleteEntity("urn:ngsi-ld:User:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:02".toUri())
    }

    @Test
    fun `it should create admin links to entities for a client`() {
        createEntity(
            "urn:ngsi-ld:Client:01".toUri(), listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2"
                )
            )
        )
        createEntity("urn:ngsi-ld:Apiary:01".toUri(), listOf("Apiary"), mutableListOf())
        createEntity("urn:ngsi-ld:Apiary:02".toUri(), listOf("Apiary"), mutableListOf())

        val targetIds = listOf("urn:ngsi-ld:Apiary:01", "urn:ngsi-ld:Apiary:02").toListOfUri()

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            "eb5599a3-513e-45ea-a78f-f9dd1e2bcca2".toUri(),
            targetIds.map {
                Relationship(
                    type = listOf(R_CAN_ADMIN),
                    datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
                )
            },
            targetIds
        )

        assert(createdRelations.size == 2)

        neo4jRepository.deleteEntity("urn:ngsi-ld:Client:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:01".toUri())
        neo4jRepository.deleteEntity("urn:ngsi-ld:Apiary:02".toUri())
    }

    fun createEntity(id: URI, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        return entityRepository.save(entity)
    }

    fun createRelationship(subjectNodeInfo: SubjectNodeInfo, relationshipType: String, objectId: URI): Relationship {
        val relationship = Relationship(type = listOf(relationshipType))

        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
