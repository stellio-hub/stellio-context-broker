package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.config.SUBJECT_GROUPS_CACHE
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
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AccessRight.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SID
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_READ
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_WRITE
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_IS_MEMBER_OF
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy.AUTH_READ
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
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
    private val offset = 0
    private val limit = 20

    @AfterEach
    fun cleanData() {
        entityRepository.deleteAll()
        // clear the caches between each test to avoid side effects with cached data
        cacheManager.cacheNames.forEach { cacheManager.getCache(it)?.clear() }
    }

    @Test
    fun `it should cache the entity URI for a user sub`() {
        createEntity(userUri, listOf(USER_TYPE))

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
    fun `it should clear a subject from all caches when it is deleted`() {
        createEntity(userUri, listOf(USER_TYPE))
        neo4jAuthorizationRepository.getSubjectRoles(userUri)
        neo4jAuthorizationRepository.getSubjectGroups(userUri)

        neo4jAuthorizationRepository.evictSubject(userUri)

        assertNull(cacheManager.getCache(SUBJECT_URI_CACHE)?.get(userUri))
        assertNull(cacheManager.getCache(SUBJECT_ROLES_CACHE)?.get(userUri))
        assertNull(cacheManager.getCache(SUBJECT_GROUPS_CACHE)?.get(userUri))
    }

    @Test
    fun `it should filter entities authorized for user with given rights`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"))

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
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"))

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
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val groupEntity = createEntity(groupUri, listOf(GROUP_TYPE))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"))
        val apiaryEntity2 = createEntity(apiary02Uri, listOf("Apiary"))

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
            mutableListOf(Property(name = AUTH_PROP_SID, value = serviceAccountUri))
        )
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"))

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
            mutableListOf(Property(name = AUTH_PROP_SID, value = serviceAccountUri))
        )
        val apiaryEntity = createEntity(apiaryUri, listOf("Apiary"))

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
            mutableListOf(Property(name = AUTH_PROP_SAP, value = AUTH_READ.name))
        )
        createEntity(apiary02Uri, listOf("Apiary"))

        val authorizedEntities =
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                listOf(apiaryUri, apiary02Uri),
                listOf(AUTH_READ.name)
            )

        assertEquals(listOf(apiaryUri), authorizedEntities)
    }

    @Test
    fun `it should filter entities with auth write if auth read and auth write are asked for`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(Property(name = AUTH_PROP_SAP, value = SpecificAccessPolicy.AUTH_WRITE.name))
        )

        val authorizedEntities =
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                listOf(apiaryUri, apiary02Uri),
                listOf(SpecificAccessPolicy.AUTH_WRITE.name, AUTH_READ.name)
            )

        assertEquals(listOf(apiaryUri), authorizedEntities)
    }

    @Test
    fun `it should filter entities with auth write if only auth write is asked for`() {
        createEntity(
            apiaryUri, listOf("Apiary"),
            mutableListOf(Property(name = AUTH_PROP_SAP, value = SpecificAccessPolicy.AUTH_WRITE.name))
        )
        createEntity(
            apiary02Uri, listOf("Apiary"),
            mutableListOf(Property(name = AUTH_PROP_SAP, value = AUTH_READ.name))
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
    fun `it should clear and populate the cache of subject's roles`() {
        val property = Property(name = AUTH_PROP_ROLES, value = listOf("admin", "creator"))
        val entity = createEntity(
            userUri,
            listOf(USER_TYPE),
            mutableListOf(property)
        )

        neo4jAuthorizationRepository.getSubjectRoles(userUri)

        val updatedEntity = entity.copy(properties = mutableListOf(property.copy(value = listOf("admin"))))
        entityRepository.save(updatedEntity)

        neo4jAuthorizationRepository.resetRolesCache()

        assertNull(cacheManager.getCache(SUBJECT_ROLES_CACHE)?.get(userUri))

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
                Property(name = AUTH_PROP_ROLES, value = listOf("admin", "creator")),
                Property(name = AUTH_PROP_SID, value = serviceAccountUri)
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
            mutableListOf(Property(name = AUTH_PROP_ROLES, value = "admin"))
        )

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin"), roles)
    }

    @Test
    fun `it should get all user's roles from group`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))

        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = AUTH_PROP_ROLES, value = listOf("admin")))
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
            mutableListOf(Property(name = AUTH_PROP_ROLES, value = "admin"))
        )

        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = AUTH_PROP_ROLES, value = listOf("creator")))
        )

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin", "creator"), roles)
    }

    @Test
    fun `it should get a user's single role from group`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))

        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = AUTH_PROP_ROLES, value = "admin"))
        )

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assertEquals(setOf("admin"), roles)
    }

    @Test
    fun `it should find no user roles`() {
        createEntity(userUri, listOf(USER_TYPE))

        val roles = neo4jAuthorizationRepository.getSubjectRoles(userUri)

        assert(roles.isEmpty())
    }

    @Test
    fun `it should find no client roles`() {
        createEntity(
            clientUri,
            listOf(CLIENT_TYPE),
            mutableListOf(Property(name = AUTH_PROP_SID, value = "some-uuid"))
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
                Property(name = AUTH_PROP_SID, value = "some-uuid"),
                Property(name = AUTH_PROP_ROLES, value = listOf("admin", "creator"))
            )
        )

        val roles = neo4jAuthorizationRepository.getSubjectRoles("urn:ngsi-ld:User:unknown".toUri())

        assert(roles.isEmpty())
    }

    @Test
    fun `it should find all groups for a subject`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val groupEntity = createEntity(groupUri, listOf(GROUP_TYPE))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val groups = neo4jAuthorizationRepository.getSubjectGroups(userUri)

        assertEquals(1, groups.size)
        assertEquals(groupUri, groups.first())
    }

    @Test
    fun `it should fill and update the cache of a subject's groups`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val groupEntity = createEntity(groupUri, listOf(GROUP_TYPE))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val groups = neo4jAuthorizationRepository.getSubjectGroups(userUri)

        assertEquals(1, groups.size)
        assertEquals(groupUri, groups.first())

        val groupEntity2 = createEntity(
            "urn:ngsi-ld:Group:02".toUri(),
            listOf(GROUP_TYPE),
            mutableListOf()
        )

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity2.id)

        neo4jAuthorizationRepository.updateSubjectGroups(userUri)

        val updatedGroups = neo4jAuthorizationRepository.getSubjectGroups(userUri)

        assertEquals(2, updatedGroups.size)
        assertTrue(updatedGroups.containsAll(setOf(groupEntity.id, groupEntity2.id)))
    }

    @Test
    fun `it should create admin links to entities`() {
        createEntity(userUri, listOf(USER_TYPE))
        createEntity(apiaryUri, listOf("Apiary"))
        createEntity(apiary02Uri, listOf("Apiary"))

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
            mutableListOf(Property(name = AUTH_PROP_SID, value = serviceAccountUri))
        )
        createEntity(apiaryUri, listOf("Apiary"))
        createEntity(apiary02Uri, listOf("Apiary"))

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
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val targetEntity = createEntity(apiaryUri, listOf("Apiary"))
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, targetEntity.id)

        val result = neo4jAuthorizationRepository.removeUserRightsOnEntity(userEntity.id, targetEntity.id)

        assertEquals(1, result)

        neo4jRepository.deleteEntity(userUri)
        neo4jRepository.deleteEntity(apiaryUri)
    }

    @Test
    fun `it should return all authorized entities that user have rights on`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val firstEntity = createEntity("urn:ngsi-ld:Beekeeper:1230".toUri(), listOf("Beekeeper"))
        val secondEntity = createEntity("urn:ngsi-ld:Beekeeper:1231".toUri(), listOf("Beekeeper"))
        val thirdEntity = createEntity("urn:ngsi-ld:Beekeeper:1232".toUri(), listOf("Beekeeper"))
        val fourthEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1233".toUri(),
            listOf("Beekeeper"),
            mutableListOf(Property(name = AUTH_PROP_SAP, value = AUTH_READ.name))
        )
        createEntity("urn:ngsi-ld:Beekeeper:1234".toUri(), listOf("Beekeeper"))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id, true)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_ADMIN, secondEntity.id, true)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, thirdEntity.id, true)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_ADMIN, fourthEntity.id, true)

        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesWithAuthentication(
            QueryParams(offset = offset, limit = limit),
            listOf(userUri.toString())
        )

        assertEquals(4, result.first)
        assertEquals(4, result.second.size)
        assertEquals(
            listOf(firstEntity.id, secondEntity.id, thirdEntity.id, fourthEntity.id),
            result.second.map { it.id }
        )
        assertTrue(result.second.all { it.type.size == 1 && it.type[0] == "Beekeeper" && it.datasetId != null })
        assertTrue(result.second.find { it.id == secondEntity.id }?.right == R_CAN_ADMIN)
        assertTrue(result.second.filter { it.id != fourthEntity.id }.none { it.specificAccessPolicy != null })
        assertTrue(result.second.find { it.id == fourthEntity.id }?.specificAccessPolicy == AUTH_READ)
    }

    @Test
    fun `it should return authorized entities filtered on specific rights`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val firstEntity = createEntity("urn:ngsi-ld:Beekeeper:1232".toUri(), listOf("Beekeeper"))
        val secondEntity = createEntity("urn:ngsi-ld:Beekeeper:1231".toUri(), listOf("Beekeeper"))
        val thirdEntity = createEntity("urn:ngsi-ld:Beekeeper:1230".toUri(), listOf("Beekeeper"))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_ADMIN, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, thirdEntity.id)

        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesWithAuthentication(
            QueryParams(q = "rCanWrite;rCanRead", offset = offset, limit = limit),
            listOf(userUri.toString())
        )

        assertEquals(2, result.first)
        assertEquals(2, result.second.size)
        assertTrue(result.second.find { it.id == firstEntity.id }?.right == R_CAN_WRITE)
        assertTrue(result.second.find { it.id == thirdEntity.id }?.right == R_CAN_READ)
    }

    @Test
    fun `it should return authorized entities filtered on entity type`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val firstEntity = createEntity("urn:ngsi-ld:Beekeeper:1230".toUri(), listOf("Beekeeper"))
        val secondEntity = createEntity("urn:ngsi-ld:Beekeeper:1231".toUri(), listOf("Beekeeper"))
        val thirdEntity = createEntity("urn:ngsi-ld:Beehive:1232".toUri(), listOf("Beehive"))
        createEntity("urn:ngsi-ld:Beekeeper:1233".toUri(), listOf("Beekeeper"))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_ADMIN, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, thirdEntity.id)

        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesWithAuthentication(
            QueryParams(types = setOf("Beekeeper"), offset = offset, limit = limit),
            listOf(userUri.toString())
        )

        assertEquals(2, result.first)
        assertEquals(2, result.second.size)
        assertTrue(result.second.all { it.type.size == 1 && it.type[0] == "Beekeeper" && it.datasetId == null })
        assertTrue(result.second.find { it.id == firstEntity.id }?.right == R_CAN_WRITE)
        assertTrue(result.second.find { it.id == secondEntity.id }?.right == R_CAN_ADMIN)
    }

    @Test
    fun `it should return entities with authorized users for a stellio-admin user`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val userEntity2 = createEntity("urn:ngsi-ld:User:02".toUri(), listOf(USER_TYPE), mutableListOf())

        val firstEntity = createEntity("urn:ngsi-ld:Beekeeper:1230".toUri(), listOf("Beekeeper"))
        val secondEntity = createEntity("urn:ngsi-ld:Beekeeper:1231".toUri(), listOf("Beekeeper"))
        val thirdEntity = createEntity(
            "urn:ngsi-ld:Beekeeper:1232".toUri(),
            listOf("Beehive"),
            mutableListOf(Property(name = AUTH_PROP_SAP, value = AUTH_READ.name))
        )
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id, true)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_ADMIN, secondEntity.id, true)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, thirdEntity.id, true)
        createRelationship(EntitySubjectNode(userEntity2.id), AUTH_REL_CAN_WRITE, firstEntity.id, true)

        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesForAdmin(
            QueryParams(offset = offset, limit = limit)
        )
        val users = listOf(userUri, "urn:ngsi-ld:User:02".toUri())

        assertEquals(3, result.first)
        assertEquals(3, result.second.size)
        assertTrue(result.second.all { it.right == R_CAN_ADMIN && it.datasetId != null })
        assertTrue(result.second.find { it.id == firstEntity.id }?.rCanWriteUsers?.size == 2)
        assertTrue(result.second.find { it.id == firstEntity.id }?.rCanWriteUsers == users)
        assertTrue(result.second.find { it.id == secondEntity.id }?.rCanAdminUsers?.size == 1)
        assertTrue(result.second.find { it.id == thirdEntity.id }?.rCanReadUsers?.size == 1)
        assertTrue(result.second.find { it.id == thirdEntity.id }?.specificAccessPolicy == AUTH_READ)
    }

    @Test
    fun `it should return authorized entities with authorized users for entities user is admin of`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val userEntity2 = createEntity("urn:ngsi-ld:User:02".toUri(), listOf(USER_TYPE), mutableListOf())

        val firstEntity = createEntity("urn:ngsi-ld:Beekeeper:1230".toUri(), listOf("Beekeeper"))
        val secondEntity = createEntity("urn:ngsi-ld:Beekeeper:1231".toUri(), listOf("Beekeeper"))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_ADMIN, firstEntity.id)
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_READ, secondEntity.id)
        createRelationship(EntitySubjectNode(userEntity2.id), AUTH_REL_CAN_WRITE, firstEntity.id)

        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesWithAuthentication(
            QueryParams(offset = offset, limit = limit),
            listOf(userUri.toString())
        )

        assertEquals(2, result.first)
        assertEquals(2, result.second.size)
        assertTrue(result.second.all { it.datasetId == null })
        assertTrue(result.second.find { it.id == firstEntity.id }?.right == R_CAN_ADMIN)
        assertTrue(result.second.find { it.id == firstEntity.id }?.rCanAdminUsers?.size == 1)
        assertTrue(result.second.find { it.id == firstEntity.id }?.rCanAdminUsers == listOf(userEntity.id))
        assertTrue(result.second.find { it.id == firstEntity.id }?.rCanReadUsers?.size == 0)
        assertTrue(result.second.find { it.id == firstEntity.id }?.rCanWriteUsers?.size == 1)
        assertTrue(result.second.find { it.id == firstEntity.id }?.rCanWriteUsers == listOf(userEntity2.id))
        assertTrue(result.second.find { it.id == secondEntity.id }?.right == R_CAN_READ)
        assertTrue(
            result.second.filter { it.id != firstEntity.id }
                .none { it.rCanAdminUsers != null && it.rCanReadUsers != null && it.rCanWriteUsers != null }
        )
    }

    @Test
    fun `it should return none entities if user is stellio-admin and limit is 0`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val firstEntity = createEntity("urn:ngsi-ld:Beekeeper:1230".toUri(), listOf("Beekeeper"))
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)

        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesForAdmin(
            QueryParams(offset = offset, limit = 0)
        )

        assertEquals(1, result.first)
        assertEquals(0, result.second.size)
    }

    @Test
    fun `it should return none entities if limit is 0`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE), mutableListOf())
        val firstEntity = createEntity("urn:ngsi-ld:Beekeeper:1230".toUri(), listOf("Beekeeper"))

        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_CAN_WRITE, firstEntity.id)

        val result = neo4jAuthorizationRepository.getAuthorizedEntitiesWithAuthentication(
            QueryParams(offset = offset, limit = 0),
            listOf(userUri.toString())
        )

        assertEquals(1, result.first)
        assertEquals(0, result.second.size)
    }

    @Test
    fun `it should return the group the user belongs to`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = JsonLdUtils.NGSILD_NAME_PROPERTY, value = "egm"))
        )
        createEntity(
            "urn:ngsi-ld:Group:02".toUri(),
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = JsonLdUtils.NGSILD_NAME_PROPERTY, value = "stellio"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val subjectGroups = neo4jAuthorizationRepository.getSubjectGroups(userUri)
        val groupEntities = neo4jAuthorizationRepository.getGroups(subjectGroups, offset, limit)

        assertEquals(1, groupEntities.first)
        assertEquals(1, groupEntities.second.size)
        assertEquals(groupUri, groupEntities.second.first().id)
        assertEquals(GROUP_TYPE, groupEntities.second.first().type)
        assertEquals("egm", groupEntities.second.first().name)
        assertEquals(false, groupEntities.second.first().isMember)
    }

    @Test
    fun `it should return all groups with membership info if user is stellio-admin`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = JsonLdUtils.NGSILD_NAME_PROPERTY, value = "egm"))
        )
        createEntity(
            "urn:ngsi-ld:Group:02".toUri(),
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = JsonLdUtils.NGSILD_NAME_PROPERTY, value = "stellio"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val subjectGroups = neo4jAuthorizationRepository.getSubjectGroups(userUri)
        val groupEntities = neo4jAuthorizationRepository.getGroupsForAdmin(subjectGroups, offset, limit)

        assertEquals(2, groupEntities.first)
        assertEquals(2, groupEntities.second.size)
        assertTrue(groupEntities.second.find { it.id == groupUri }?.isMember == true)
        assertTrue(groupEntities.second.find { it.id == "urn:ngsi-ld:Group:02".toUri() }?.isMember == false)
    }

    @Test
    fun `it should return none groups if limit is 0`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = JsonLdUtils.NGSILD_NAME_PROPERTY, value = "egm"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val subjectGroups = neo4jAuthorizationRepository.getSubjectGroups(userUri)
        val groupEntities = neo4jAuthorizationRepository.getGroups(subjectGroups, offset, 0)

        assertEquals(1, groupEntities.first)
        assertEquals(0, groupEntities.second.size)
    }

    @Test
    fun `it should return none groups if user is stellio-admin and limit is 0`() {
        val userEntity = createEntity(userUri, listOf(USER_TYPE))
        val groupEntity = createEntity(
            groupUri,
            listOf(GROUP_TYPE),
            mutableListOf(Property(name = JsonLdUtils.NGSILD_NAME_PROPERTY, value = "egm"))
        )
        createRelationship(EntitySubjectNode(userEntity.id), AUTH_REL_IS_MEMBER_OF, groupEntity.id)

        val subjectGroups = neo4jAuthorizationRepository.getSubjectGroups(userUri)
        val groupEntities = neo4jAuthorizationRepository.getGroupsForAdmin(subjectGroups, offset, 0)

        assertEquals(1, groupEntities.first)
        assertEquals(0, groupEntities.second.size)
    }

    fun createEntity(id: URI, type: List<String>, properties: MutableList<Property> = mutableListOf()): Entity {
        val entity = Entity(id = id, types = type, properties = properties)
        return entityRepository.save(entity)
    }

    fun createRelationship(
        subjectNodeInfo: SubjectNodeInfo,
        relationshipType: String,
        objectId: URI,
        hasDatasetId: Boolean = false
    ): Relationship {
        val datasetId =
            if (hasDatasetId) "urn:ngsi-ld:datasetId:$objectId:$relationshipType".toUri()
            else null
        val relationship = Relationship(
            objectId = objectId,
            type = listOf(relationshipType),
            datasetId = datasetId
        )

        neo4jRepository.createRelationshipOfSubject(subjectNodeInfo, relationship, objectId)

        return relationship
    }
}
