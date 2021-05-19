package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.*
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
import com.egm.stellio.shared.util.JsonLdUtils.EGM_SPECIFIC_ACCESS_POLICY
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

    private val userUri = "urn:ngsi-ld:User:01".toUri()
    private val groupUri = "urn:ngsi-ld:Group:01".toUri()
    private val clientUri = "urn:ngsi-ld:Client:01".toUri()
    private val serviceAccountUri = "urn:ngsi-ld:ServiceAccount:01".toUri()
    private val apiaryUri = "urn:ngsi-ld:Apiary:01".toUri()
    private val apiary02Uri = "urn:ngsi-ld:Apiary:02".toUri()

    companion object {
        const val EGM_CAN_READ = "https://ontology.eglobalmark.com/authorization#rCanRead"
        const val EGM_CAN_WRITE = "https://ontology.eglobalmark.com/authorization#rCanWrite"
        const val EGM_CAN_ADMIN = "https://ontology.eglobalmark.com/authorization#rCanAdmin"
        const val EGM_IS_MEMBER_OF = "https://ontology.eglobalmark.com/authorization#isMemberOf"
    }

    @Test
    fun `it should filter entities authorized for user with given rights`() {
        val userEntity = createEntity(userUri, listOf("User"), mutableListOf())
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                userUri,
                listOf(apiaryUri),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(availableRightsForEntities == listOf(apiaryUri))

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(apiaryUri)
    }

    @Test
    fun `it should find no entities are authorized by user`() {
        val userEntity = createEntity(userUri, listOf("User"), mutableListOf())
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                userUri,
                listOf(apiaryUri),
                setOf(EGM_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(apiaryUri)
    }

    @Test
    fun `it should filter entities that are authorized by user's group`() {
        val userEntity = createEntity(userUri, listOf("User"), mutableListOf())
        val groupEntity = createEntity(groupUri, listOf("Group"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())
        val apiaryEntity2 = createEntity(apiary02Uri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), EGM_CAN_WRITE, apiaryEntity.id)
        createRelationship(EntitySubjectNode(groupEntity.id), EGM_CAN_READ, apiaryEntity2.id)

        val authorizedEntitiesId =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                userUri,
                listOf(apiaryUri, apiary02Uri),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(authorizedEntitiesId == listOf(apiaryUri, apiary02Uri))

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(apiaryUri)
        neo4jRepository.deleteEntity(apiary02Uri)
    }

    @Test
    fun `it should find no entities are authorized for client`() {
        val clientEntity = createEntity(
            clientUri, listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = serviceAccountUri
                )
            )
        )
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                serviceAccountUri,
                listOf(apiaryUri),
                setOf(EGM_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())

        neo4jRepository.deleteEntity(clientUri)
        neo4jRepository.deleteEntity(apiaryUri)
    }

    @Test
    fun `it should filter entities authorized for client with given rights`() {
        val clientEntity = createEntity(
            clientUri, listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = serviceAccountUri
                )
            )
        )
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), EGM_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                serviceAccountUri,
                listOf(apiaryUri),
                setOf(EGM_CAN_READ, EGM_CAN_WRITE)
            )

        assert(availableRightsForEntities == listOf(apiaryUri))

        neo4jRepository.deleteEntity(clientUri)
        neo4jRepository.deleteEntity(apiaryUri)
    }

    @Test
    fun `it should filter entities authorized per a specific access policy`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = EGM_SPECIFIC_ACCESS_POLICY,
                    value = SpecificAccessPolicy.AUTH_READ.name
                )
            )
        )
        createEntity(apiary02Uri, listOf("Apiary"), mutableListOf())

        val authorizedEntities =
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                listOf(apiaryUri, apiary02Uri),
                listOf(SpecificAccessPolicy.AUTH_READ.name)
            )

        assert(authorizedEntities == listOf(apiaryUri))

        neo4jRepository.deleteEntity(apiaryUri)
        neo4jRepository.deleteEntity(apiary02Uri)
    }

    @Test
    fun `it should filter entities with auth write if auth read and auth write are asked for`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = EGM_SPECIFIC_ACCESS_POLICY,
                    value = SpecificAccessPolicy.AUTH_WRITE.name
                )
            )
        )

        val authorizedEntities =
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                listOf(apiaryUri, apiary02Uri),
                listOf(SpecificAccessPolicy.AUTH_WRITE.name, SpecificAccessPolicy.AUTH_READ.name)
            )

        assert(authorizedEntities == listOf(apiaryUri))

        neo4jRepository.deleteEntity(apiaryUri)
    }

    @Test
    fun `it should filter entities with auth write if only auth write is asked for`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = EGM_SPECIFIC_ACCESS_POLICY,
                    value = SpecificAccessPolicy.AUTH_WRITE.name
                )
            )
        )
        createEntity(
            apiary02Uri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = EGM_SPECIFIC_ACCESS_POLICY,
                    value = SpecificAccessPolicy.AUTH_READ.name
                )
            )
        )

        val authorizedEntities =
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                listOf(apiaryUri, apiary02Uri),
                listOf(SpecificAccessPolicy.AUTH_WRITE.name)
            )

        assert(authorizedEntities == listOf(apiaryUri))

        neo4jRepository.deleteEntity(apiaryUri)
        neo4jRepository.deleteEntity(apiary02Uri)
    }

    @Test
    fun `it should get all user's roles`() {
        createEntity(
            userUri,
            listOf("User"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin", "creator")
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles(userUri)

        assert(roles == setOf("admin", "creator"))

        neo4jRepository.deleteEntity(userUri)
    }

    @Test
    fun `it should get all client's roles`() {
        createEntity(
            clientUri,
            listOf("Client"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin", "creator")
                ),
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = serviceAccountUri
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles(serviceAccountUri)

        assert(roles == setOf("admin", "creator"))

        neo4jRepository.deleteEntity(clientUri)
    }

    @Test
    fun `it should get a user's single role`() {
        createEntity(
            userUri,
            listOf("User"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles(userUri)

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity(userUri)
    }

    @Test
    fun `it should get all user's roles from group`() {
        val userEntity = createEntity(userUri, listOf("User"), mutableListOf())

        val groupEntity = createEntity(
            groupUri,
            listOf("Group"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = listOf("admin")
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getUserRoles(userUri)

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(groupUri)
    }

    @Test
    fun `it should get a user's single role from group`() {
        val userEntity = createEntity(userUri, listOf("User"), mutableListOf())

        val groupEntity = createEntity(
            groupUri,
            listOf("Group"),
            mutableListOf(
                Property(
                    name = EGM_ROLES,
                    value = "admin"
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), EGM_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getUserRoles(userUri)

        assert(roles == setOf("admin"))

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(groupUri)
    }

    @Test
    fun `it should find no user roles`() {
        createEntity(userUri, listOf("User"), mutableListOf())

        val roles = neo4jAuthorizationRepository.getUserRoles(userUri)

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity(userUri)
    }

    @Test
    fun `it should find no client roles`() {
        createEntity(
            clientUri,
            listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = "some-uuid"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:unknown".toUri())

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity(clientUri)
    }

    @Test
    fun `it should find no client roles if the service account is not known`() {
        createEntity(
            clientUri,
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

        val roles = neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:unknown".toUri())

        assert(roles.isEmpty())

        neo4jRepository.deleteEntity(clientUri)
    }

    @Test
    fun `it should create admin links to entities`() {
        createEntity(userUri, listOf("User"), mutableListOf())
        createEntity(apiaryUri, listOf("Apiary"), mutableListOf())
        createEntity(apiary02Uri, listOf("Apiary"), mutableListOf())

        val targetIds = listOf(apiaryUri, apiary02Uri)

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            userUri,
            targetIds.map {
                Relationship(
                    type = listOf(R_CAN_ADMIN),
                    datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
                )
            },
            targetIds
        )

        assert(createdRelations.size == 2)

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(apiaryUri)
        neo4jRepository.deleteEntity(apiary02Uri)
    }

    @Test
    fun `it should create admin links to entities for a client`() {
        createEntity(
            clientUri, listOf("Client"),
            mutableListOf(
                Property(
                    name = SERVICE_ACCOUNT_ID,
                    value = serviceAccountUri
                )
            )
        )
        createEntity(apiaryUri, listOf("Apiary"), mutableListOf())
        createEntity(apiary02Uri, listOf("Apiary"), mutableListOf())

        val targetIds = listOf(apiaryUri, apiary02Uri)

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            serviceAccountUri,
            targetIds.map {
                Relationship(
                    type = listOf(R_CAN_ADMIN),
                    datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
                )
            },
            targetIds
        )

        assert(createdRelations.size == 2)

        neo4jRepository.deleteEntity(clientUri)
        neo4jRepository.deleteEntity(apiaryUri)
        neo4jRepository.deleteEntity(apiary02Uri)
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
