package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.config.SUBJECT_ROLES_CACHE
import com.egm.stellio.entity.config.SUBJECT_URI_CACHE
import com.egm.stellio.entity.config.WithNeo4jContainer
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.SubjectNodeInfo
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SID
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cache.CacheManager
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest
@ActiveProfiles("test")
class Neo4jAuthorizationRepositoryTest : WithNeo4jContainer {

    @Autowired
    private lateinit var neo4jAuthorizationRepository: Neo4jAuthorizationRepository

    @Autowired
    private lateinit var neo4jRepository: Neo4jRepository

    @Autowired
    private lateinit var entityRepository: EntityRepository

    @Autowired
    private lateinit var cacheManager: CacheManager

    private val userUri = "urn:ngsi-ld:User:01".toUri()
    private val groupUri = "urn:ngsi-ld:Group:01".toUri()
    private val clientUri = "urn:ngsi-ld:Client:01".toUri()
    private val serviceAccountUri = "urn:ngsi-ld:ServiceAccount:01".toUri()
    private val apiaryUri = "urn:ngsi-ld:Apiary:01".toUri()
    private val apiary02Uri = "urn:ngsi-ld:Apiary:02".toUri()

    @AfterEach
    fun cleanData() {
        entityRepository.deleteAll()
        // clear the caches between each test to avoid side effects with cached data
        cacheManager.cacheNames.forEach { cacheManager.getCache(it)?.clear() }
    }

    @Test
    fun `it should cache the entity URI for a user sub`() {
        createEntity(userUri, listOf(USER_TYPE), mutableListOf())

        neo4jAuthorizationRepository.getSubjectUri(userUri)

        val cachedUri = cacheManager.getCache(SUBJECT_URI_CACHE)?.get(userUri, URI::class.java)
        assertNotNull(cachedUri)
        assertEquals(userUri, cachedUri)
    }

    @Test
    fun `it should cache the entity URI for a client sub`() {
        createEntity(
            clientUri,
            listOf(CLIENT_TYPE),
            mutableListOf(Property(name = AUTH_PROP_SID, value = serviceAccountUri))
        )

        neo4jAuthorizationRepository.getSubjectUri(serviceAccountUri)

        val cachedUri = cacheManager.getCache(SUBJECT_URI_CACHE)?.get(serviceAccountUri, URI::class.java)
        assertNotNull(cachedUri)
        assertEquals(clientUri, cachedUri)
    }

    @Test
    fun `it should filter entities authorized for user with given rights`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                userUri,
                listOf(apiaryUri),
                setOf(AUTH_REL_CAN_READ, AUTH_REL_CAN_WRITE)
            )

        assertEquals(listOf(apiaryUri), availableRightsForEntities)
    }

    @Test
    fun `it should find no entities are authorized by user`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                userUri,
                listOf(apiaryUri),
                setOf(AUTH_REL_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())
    }

    @Test
    fun `it should filter entities that are authorized by user's group`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val groupEntity = createEntity(groupUri, listOf(GROUP_TYPE), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())
        val apiaryEntity2 = createEntity(apiary02Uri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, apiaryEntity.id)
        createRelationship(EntitySubjectNode(groupEntity.id), AUTH_REL_CAN_READ, apiaryEntity2.id)

        val authorizedEntitiesId =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                userUri,
                listOf(apiaryUri, apiary02Uri),
                setOf(AUTH_REL_CAN_READ, AUTH_REL_CAN_WRITE)
            )

        assertEquals(listOf(apiaryUri, apiary02Uri), authorizedEntitiesId)
    }

    @Test
    fun `it should find no entities are authorized for client`() {
        val clientEntity = createEntity(
            clientUri, listOf(CLIENT_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_SID,
                    value = serviceAccountUri
                )
            )
        )
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), AUTH_REL_CAN_WRITE, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                serviceAccountUri,
                listOf(apiaryUri),
                setOf(AUTH_REL_CAN_READ)
            )

        assert(availableRightsForEntities.isEmpty())
    }

    @Test
    fun `it should filter entities authorized for client with given rights`() {
        val clientEntity = createEntity(
            clientUri, listOf(CLIENT_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_SID,
                    value = serviceAccountUri
                )
            )
        )
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())

        createRelationship(EntitySubjectNode(clientEntity.id), AUTH_REL_CAN_READ, apiaryEntity.id)

        val availableRightsForEntities =
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                clientUri,
                listOf(apiaryUri),
                setOf(AUTH_REL_CAN_READ, AUTH_REL_CAN_WRITE)
            )

        assertEquals(listOf(apiaryUri), availableRightsForEntities)
    }

    @Test
    fun `it should filter entities authorized per a specific access policy`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = AuthContextModel.AUTH_PROP_SAP,
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

        assertEquals(listOf(apiaryUri), authorizedEntities)
    }

    @Test
    fun `it should filter entities with auth write if auth read and auth write are asked for`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = AuthContextModel.AUTH_PROP_SAP,
                    value = SpecificAccessPolicy.AUTH_WRITE.name
                )
            )
        )

        val authorizedEntities =
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                listOf(apiaryUri, apiary02Uri),
                listOf(SpecificAccessPolicy.AUTH_WRITE.name, SpecificAccessPolicy.AUTH_READ.name)
            )

        assertEquals(listOf(apiaryUri), authorizedEntities)
    }

    @Test
    fun `it should filter entities with auth write if only auth write is asked for`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = AuthContextModel.AUTH_PROP_SAP,
                    value = SpecificAccessPolicy.AUTH_WRITE.name
                )
            )
        )
        createEntity(
            apiary02Uri, listOf("Apiary"),
            mutableListOf(
                Property(
                    name = AuthContextModel.AUTH_PROP_SAP,
                    value = SpecificAccessPolicy.AUTH_READ.name
                )
            )
        )

        val authorizedEntities =
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                listOf(apiaryUri, apiary02Uri),
                listOf(SpecificAccessPolicy.AUTH_WRITE.name)
            )

        assertEquals(listOf(apiaryUri), authorizedEntities)
    }

    @Test
    fun `it should cache subject's roles`() {
        createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf(Property(name = AUTH_PROP_ROLES, value = listOf("admin", "creator")))
        )

        neo4jAuthorizationRepository.getSubjectRoles(userUri)

        val cachedRoles = cacheManager.getCache(SUBJECT_ROLES_CACHE)?.get(userUri, Set::class.java)
        assertNotNull(cachedRoles)
        assertEquals(setOf("admin", "creator"), cachedRoles)
    }

    @Test
    fun `it should update the cache of subject's roles`() {
        val property = Property(name = AUTH_PROP_ROLES, value = listOf("admin", "creator"))
        val entity = createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf(property)
        )

        neo4jAuthorizationRepository.getSubjectRoles(userUri)

        val updatedEntity = entity.copy(
            properties = mutableListOf(property.copy(value = listOf("admin")))
        )
        entityRepository.save(updatedEntity)

        neo4jAuthorizationRepository.resetRolesCache()
        neo4jAuthorizationRepository.getSubjectRoles(userUri)

        val cachedRoles = cacheManager.getCache(SUBJECT_ROLES_CACHE)?.get(userUri, Set::class.java)
        assertNotNull(cachedRoles)
        assertEquals(setOf("admin"), cachedRoles)
    }

    @Test
    fun `it should get all user's roles`() {
        createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf(Property(name = AUTH_PROP_ROLES, value = listOf("admin", "creator")))
        )

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin", "creator"), roles)
    }

    @Test
    fun `it should get all client's roles`() {
        createEntity(
            clientUri,
            listOf(CLIENT_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_ROLES,
                    value = listOf("admin", "creator")
                ),
                Property(
                    name = AUTH_PROP_SID,
                    value = serviceAccountUri
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getSubjectRoles(clientUri)

        assertEquals(setOf("admin", "creator"), roles)
    }

    @Test
    fun `it should get a user's single role`() {
        createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_ROLES,
                    value = "admin"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin"), roles)
    }

    @Test
    fun `it should get all user's roles from group`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())

        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_ROLES,
                    value = listOf("admin")
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin"), roles)
    }

    @Test
    fun `it should get all user's roles from user and group`() {
        val userEntity = createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_ROLES,
                    value = "admin"
                )
            )
        )

        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_ROLES,
                    value = listOf("creator")
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin", "creator"), roles)
    }

    @Test
    fun `it should get a user's single role from group`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())

        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_ROLES,
                    value = "admin"
                )
            )
        )

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin"), roles)
    }

    @Test
    fun `it should find no user roles`() {
        createEntity(userUri, listOf(USER_TYPE), mutableListOf())

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assert(roles.isEmpty())
    }

    @Test
    fun `it should find no client roles`() {
        createEntity(
            clientUri,
            listOf(CLIENT_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_SID,
                    value = "some-uuid"
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getSubjectRoles("urn:ngsi-ld:User:unknown".toUri())

        assert(roles.isEmpty())
    }

    @Test
    fun `it should find no client roles if the service account is not known`() {
        createEntity(
            clientUri,
            listOf(CLIENT_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_SID,
                    value = "some-uuid"
                ),
                Property(
                    name = AUTH_PROP_ROLES,
                    value = listOf("admin", "creator")
                )
            )
        )

        val roles = neo4jAuthorizationRepository.getSubjectRoles("urn:ngsi-ld:User:unknown".toUri())

        assert(roles.isEmpty())
    }

    @Test
    fun `it should find all groups for a subject`() {
        val userEntity = createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf()
        )

        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf()
        )

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val groups = neo4jAuthorizationRepository.getSubjectGroups(userUri)

        assertEquals(1, groups.size)
        assertEquals(groupUri, groups.first())
    }

    @Test
    fun `it should create admin links to entities`() {
        createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        createEntity(apiaryUri, listOf("Apiary"), mutableListOf())
        createEntity(apiary02Uri, listOf("Apiary"), mutableListOf())

        val targetIds = listOf(apiaryUri, apiary02Uri)

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            userUri,
            targetIds.map {
                Relationship(
                    objectId = it,
                    type = listOf(AUTH_REL_CAN_ADMIN),
                    datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
                )
            },
            targetIds
        )

        assertEquals(2, createdRelations.size)
    }

    @Test
    fun `it should create admin links to entities for a client`() {
        createEntity(
            clientUri,
            listOf(CLIENT_TYPE),
            mutableListOf(
                Property(
                    name = AUTH_PROP_SID,
                    value = serviceAccountUri
                )
            )
        )
        createEntity(apiaryUri, listOf("Apiary"), mutableListOf())
        createEntity(apiary02Uri, listOf("Apiary"), mutableListOf())

        val targetIds = listOf(apiaryUri, apiary02Uri)

        val createdRelations = neo4jAuthorizationRepository.createAdminLinks(
            clientUri,
            targetIds.map {
                Relationship(
                    objectId = it,
                    type = listOf(AUTH_REL_CAN_ADMIN),
                    datasetId = "urn:ngsi-ld:Dataset:rCanAdmin:$it".toUri()
                )
            },
            targetIds
        )

        assertEquals(2, createdRelations.size)
    }

    @Test
    fun `it should remove an user's rights on an entity`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val targetEntity = createEntity(apiaryUri, listOf("Apiary"), mutableListOf())
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, targetEntity.id)

        val result = neo4jAuthorizationRepository.removeUserRightsOnEntity(userEntity.id, targetEntity.id)

        assertEquals(1, result)

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(apiaryUri)
    }

    fun createEntity(id: URI, type: List<String>, properties: MutableList<Property>): Entity {
        val entity = Entity(id = id, type = type, properties = properties)
        return entityRepository.save(entity)
    }

    fun createRelationship(subjectNodeInfo: SubjectNodeInfo, relationshipType: String, objectId: URI): Relationship {
        val relationship = Relationship(objectId = objectId, type = listOf(relationshipType))

        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
