package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.config.SUBJECT_GROUPS_CACHE
import com.egm.stellio.entity.config.SUBJECT_ROLES_CACHE
import com.egm.stellio.entity.config.SUBJECT_URI_CACHE
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.repository.QueryUtils.buildMatchEntityClause
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.ALL_IAM_RIGHTS_TERMS
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_ROLES
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SID
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.CLIENT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import org.neo4j.driver.internal.value.DateTimeValue
import org.neo4j.driver.internal.value.StringValue
import org.neo4j.driver.types.Node
import org.springframework.cache.annotation.CacheEvict
import org.springframework.cache.annotation.CachePut
import org.springframework.cache.annotation.Cacheable
import org.springframework.cache.annotation.Caching
import org.springframework.data.neo4j.core.Neo4jClient
import org.springframework.stereotype.Component
import java.net.URI

@Component
class Neo4jAuthorizationRepository(
    private val neo4jClient: Neo4jClient
) {

    @Cacheable(SUBJECT_URI_CACHE)
    fun getSubjectUri(defaultSubUri: URI): URI {
        val query =
            """
            MATCH (entity:`$USER_TYPE`)
            WHERE entity.id = ${'$'}defaultSubUri
            RETURN entity.id as id
            UNION
            MATCH (entity:`$CLIENT_TYPE`)-[:HAS_VALUE]->(p:Property { name: "$AUTH_PROP_SID" })
            WHERE p.value = ${'$'}defaultSubUri
            RETURN entity.id as id
            """.trimIndent()

        val parameters = mapOf(
            "defaultSubUri" to defaultSubUri.toString()
        )

        return neo4jClient.query(query).bindAll(parameters)
            .fetch()
            .one()
            .map { (it["id"] as String).toUri() }
            .orElse(defaultSubUri)
    }

    @Caching(
        evict = [
            CacheEvict(value = arrayOf(SUBJECT_URI_CACHE)),
            CacheEvict(value = arrayOf(SUBJECT_ROLES_CACHE)),
            CacheEvict(value = arrayOf(SUBJECT_GROUPS_CACHE))
        ]
    )
    fun evictSubject(subjectId: URI) = Unit

    fun filterEntitiesUserHasOneOfGivenRights(
        subjectId: URI,
        entitiesId: List<URI>,
        rights: Set<String>
    ): List<URI> {
        val query =
            """
            MATCH (userEntity:Entity { id: ${'$'}subjectId })
            WITH userEntity 
            MATCH (entity:Entity)
            WHERE entity.id IN ${'$'}entitiesId
            WITH userEntity, entity
            CALL {
                WITH userEntity, entity
                MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->(entity:Entity)
                WHERE size([label IN labels(right) WHERE label IN ${'$'}rights]) > 0
                return entity.id as id
                UNION
                WITH userEntity, entity
                MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)-
                    [:isMemberOf]->(:Entity)-[:HAS_OBJECT]-(grpRight:Attribute:Relationship)-[]->(entity:Entity)
                WHERE size([label IN labels(grpRight) WHERE label IN ${'$'}rights]) > 0
                return entity.id as id
            }
            RETURN id
            """.trimIndent()

        val parameters = mapOf(
            "subjectId" to subjectId.toString(),
            "entitiesId" to entitiesId.toListOfString(),
            "rights" to rights
        )

        return neo4jClient.query(query).bindAll(parameters)
            .fetch().all()
            .map { (it["id"] as String).toUri() }
    }

    fun filterEntitiesWithSpecificAccessPolicy(
        entitiesId: List<URI>,
        specificAccessPolicies: List<String>
    ): List<URI> {
        val query =
            """
            MATCH (entity:Entity)
            WHERE entity.id IN ${'$'}entitiesId
            MATCH (entity)-[:HAS_VALUE]->(p:Property { name: "$AUTH_PROP_SAP" })
            WHERE p.value IN ${'$'}specificAccessPolicies
            RETURN entity.id as id
            """.trimIndent()

        val parameters = mapOf(
            "entitiesId" to entitiesId.toListOfString(),
            "specificAccessPolicies" to specificAccessPolicies
        )

        return neo4jClient.query(query).bindAll(parameters)
            .fetch().all()
            .map { (it["id"] as String).toUri() }
    }

    @Cacheable(SUBJECT_ROLES_CACHE)
    fun getSubjectRoles(subjectId: URI): Set<String> {
        val query =
            """
            MATCH (userEntity:Entity { id: ${'$'}subjectId })
            OPTIONAL MATCH (userEntity)-[:HAS_VALUE]->(p:Property { name:"$AUTH_PROP_ROLES" })
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]-(r:Attribute:Relationship)-
                [:isMemberOf]->(group:Entity)-[:HAS_VALUE]->(pgroup:Property { name: "$AUTH_PROP_ROLES" })
            RETURN apoc.coll.union(collect(p.value), collect(pgroup.value)) as roles
            """.trimIndent()

        val parameters = mapOf(
            "subjectId" to subjectId.toString()
        )

        val result = neo4jClient.query(query).bindAll(parameters).fetch().all()

        return result
            .flatMap {
                val roles = it["roles"] as List<*>
                roles.flatMap { rolesEntry ->
                    when (rolesEntry) {
                        is String -> listOf(rolesEntry)
                        is List<*> -> rolesEntry as List<String>
                        else -> null
                    }
                }
            }
            .toSet()
    }

    /**
     * As a role can be given to a group, this would require to first get all the members of the group,
     * and then to reset the cache entry for every member, so just simply evict the whole cache.
     */
    @CacheEvict(value = [SUBJECT_ROLES_CACHE], allEntries = true)
    fun resetRolesCache() = Unit

    @Cacheable(SUBJECT_GROUPS_CACHE)
    fun getSubjectGroups(subjectId: URI): Set<URI> {
        val query =
            """
            MATCH (userEntity:Entity { id: ${'$'}subjectId })
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]-(r:Attribute:Relationship)-[:isMemberOf]->(group:Entity)
            RETURN collect(group.id) as groupsUris
            """.trimIndent()

        val parameters = mapOf(
            "subjectId" to subjectId.toString()
        )

        val result = neo4jClient.query(query).bindAll(parameters).fetch().one()

        return result
            .map {
                (it["groupsUris"] as List<String>).map { it.toUri() }.toSet()
            }
            .orElse(emptySet())
    }

    @CachePut(SUBJECT_GROUPS_CACHE)
    fun updateSubjectGroups(subjectId: URI): Set<URI> = getSubjectGroups(subjectId)

    fun createAdminLinks(subjectId: URI, relationships: List<Relationship>, entitiesId: List<URI>): List<URI> {
        val query =
            """
            MATCH (user:Entity { id: ${'$'}subjectId })
            WITH user
            UNWIND ${'$'}relPropsAndTargets AS relPropAndTarget
            MATCH (target:Entity { id: relPropAndTarget.targetEntityId })
            CREATE (user)-[:HAS_OBJECT]->(r:Attribute:Relationship:`$AUTH_REL_CAN_ADMIN`)-[:rCanAdmin]->(target)
            SET r = relPropAndTarget.props
            RETURN r.id as id
            """

        val parameters = mapOf(
            "relPropsAndTargets" to relationships
                .map { it.nodeProperties() }
                .zip(entitiesId.toListOfString())
                .map {
                    mapOf("props" to it.first, "targetEntityId" to it.second)
                },
            "subjectId" to subjectId.toString()
        )

        return neo4jClient.query(query).bindAll(parameters)
            .fetch().all()
            .map { (it["id"] as String).toUri() }
    }

    fun removeUserRightsOnEntity(
        subjectId: URI,
        targetId: URI
    ): Int {
        val matchQuery =
            """
            MATCH (subject:Entity { id: ${'$'}subjectId })-[:HAS_OBJECT]-(relNode)
                    -[]->(target:Entity { id: ${'$'}targetId })
            DETACH DELETE relNode
            """.trimIndent()

        val parameters = mapOf(
            "subjectId" to subjectId.toString(),
            "targetId" to targetId.toString()
        )

        return neo4jClient.query(matchQuery).bindAll(parameters).run().counters().nodesDeleted()
    }

    fun getAuthorizedEntitiesWithAuthentication(
        queryParams: QueryParams,
        userAndGroupIds: List<String>
    ): Pair<Int, List<EntityAccessControl>> {
        val matchEntityClause = buildMatchEntityClause(queryParams.types?.first(), prefix = "")
        val authTerm = buildAuthTerm(queryParams.q)
        val matchAuthorizedEntitiesClause =
            """
            MATCH (user)
            WHERE user.id IN ${'$'}userAndGroupIds
            MATCH (user)-[]->()-[right:$authTerm]->$matchEntityClause
            OPTIONAL MATCH $matchEntityClause-[:HAS_VALUE]->(prop:Property { name: '$AUTH_PROP_SAP' })
            WITH entity, right, prop.value as specificAccessPolicy
            ORDER BY entity.id
            """.trimIndent()

        val pagingClause = if (queryParams.limit == 0)
            """
            RETURN count(distinct(entity)) as count
            """.trimIndent()
        else
            """
            RETURN 
                collect(distinct({
                    entity: entity, 
                    right: right, 
                    specificAccessPolicy: specificAccessPolicy
                })) as entities, 
                count(distinct(entity)) as count
            SKIP ${queryParams.offset} LIMIT ${queryParams.limit}
            """.trimIndent()

        val result = neo4jClient
            .query(
                """
                $matchAuthorizedEntitiesClause
                $pagingClause
                """
            )
            .bind(userAndGroupIds).to("userAndGroupIds")
            .fetch()
            .all()

        return prepareResultsAuthorizedEntities(result, queryParams.limit)
    }

    // User is admin, so we return `rCanAdmin` as right
    fun getAuthorizedEntitiesForAdmin(
        queryParams: QueryParams
    ): Pair<Int, List<EntityAccessControl>> {
        val matchEntityClause = buildMatchEntityClause(queryParams.types?.first(), "")
        val matchAuthorizedEntitiesClause =
            """
            MATCH $matchEntityClause
            WHERE (
                NOT '$USER_TYPE' IN labels(entity) AND
                NOT '$GROUP_TYPE' IN labels(entity) AND
                NOT '$CLIENT_TYPE' IN labels(entity)
            )
            OPTIONAL MATCH $matchEntityClause-[:HAS_VALUE]->(prop:Property { name: '$AUTH_PROP_SAP' })
            WITH entity, prop.value as specificAccessPolicy
            ORDER BY entity.id
            """.trimIndent()

        val pagingClause = if (queryParams.limit == 0)
            """
            RETURN count(entity) as count
            """.trimIndent()
        else
            """
            RETURN 
                collect(distinct({
                    entity: entity, 
                    right: "$AUTH_TERM_CAN_ADMIN", 
                    specificAccessPolicy: specificAccessPolicy
                })) as entities, 
                count(distinct(entity)) as count 
            SKIP ${queryParams.offset} LIMIT ${queryParams.limit}
            """.trimIndent()

        val result = neo4jClient
            .query(
                """
                $matchAuthorizedEntitiesClause
                $pagingClause
                """
            )
            .fetch()
            .all()

        return prepareResultsAuthorizedEntities(result, queryParams.limit)
    }

    fun getEntityRightsPerUser(entityId: URI): List<Map<String, URI>> {
        val authTerm = buildAuthTerm(null)
        val query =
            """
            MATCH (user)-[]->()-[right:$authTerm]->(entity:Entity { id: ${'$'}entityId })
            WITH user, right
            ORDER BY user.id
            RETURN collect(distinct(user.id)) as usersIds, collect(distinct(right)) as rights
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to entityId.toString()
        )

        val result = neo4jClient.query(query).bindAll(parameters).fetch().one().get()

        val usersIds = (result["usersIds"] as List<String>).map { it.toUri() }
        val rights = (result["rights"] as List<org.neo4j.driver.types.Relationship>).map { it.type() }

        return rights
            .zip(usersIds)
            .map { mapOf(it.first to it.second) }
    }

    fun buildAuthTerm(q: String?): String =
        q?.replace(qPattern.toRegex()) { matchResult ->
            matchResult.value
        }?.replace(";", "|")
            ?: ALL_IAM_RIGHTS_TERMS.joinToString("|")

    fun prepareResultsAuthorizedEntities(
        result: Collection<Map<String, Any>>,
        limit: Int
    ): Pair<Int, List<EntityAccessControl>> =
        if (limit == 0)
            Pair((result.first()["count"] as Long).toInt(), emptyList())
        else
            Pair(
                (result.first()["count"] as Long).toInt(),
                toEntityAccessControl((result.firstOrNull()?.get("entities") as List<Map<String, Any>>))
            )

    fun toEntityAccessControl(entities: List<Map<String, Any>>): List<EntityAccessControl> =
        entities.map {
            val entityNode = it["entity"] as Node
            val rightOnEntity =
                if (it["right"] is org.neo4j.driver.types.Relationship)
                    (it["right"] as org.neo4j.driver.types.Relationship).type()
                else it["right"] as String
            val specificAccessPolicy = it["specificAccessPolicy"] as String?
            val entityId = (entityNode.get("id") as StringValue).asString().toUri()
            val userRightOnEntity = AccessRight.forAttributeName(rightOnEntity).orNull()!!

            val usersRightsOnEntity =
                if (userRightOnEntity == AccessRight.R_CAN_ADMIN)
                    getEntityRightsPerUser(entityId)
                else null

            EntityAccessControl(
                id = entityId,
                type = entityNode
                    .labels()
                    .toList()
                    .filter { !AuthContextModel.IAM_TYPES.plus("Entity").contains(it) },
                right = userRightOnEntity,
                rCanAdminUsers = usersRightsOnEntity.valuesForRight(AccessRight.R_CAN_ADMIN),
                rCanWriteUsers = usersRightsOnEntity.valuesForRight(AccessRight.R_CAN_WRITE),
                rCanReadUsers = usersRightsOnEntity.valuesForRight(AccessRight.R_CAN_READ),
                createdAt = (entityNode.get("createdAt") as DateTimeValue).asZonedDateTime(),
                modifiedAt = (entityNode.get("modifiedAt") as? DateTimeValue)?.asZonedDateTime(),
                specificAccessPolicy = specificAccessPolicy?.let { it ->
                    AuthContextModel.SpecificAccessPolicy.valueOf(it)
                }
            )
        }

    private fun List<Map<String, URI>>?.valuesForRight(accessRight: AccessRight): List<URI>? =
        this?.filter { it.containsKey(accessRight.attributeName) }
            ?.map { it.getValue(accessRight.attributeName) }

    fun getGroups(groupsIds: Set<URI>, offset: Int, limit: Int): Pair<Int, List<Group>> {
        val matchAuthorizedGroupsClause =
            """
            MATCH (group:`$GROUP_TYPE`)
            WHERE group.id IN ${'$'}groupsIds
            MATCH (group)-[:HAS_VALUE]->(p:Property { name:"$NGSILD_NAME_PROPERTY" })
            """.trimIndent()

        val pagingClause = if (limit == 0)
            """
            RETURN count(distinct(group)) as count
            """.trimIndent()
        else
            """
            WITH group, p
            ORDER BY group.id
            return 
                collect(distinct({
                    groupId:group.id, groupName:p.value
                })) as groups, 
                count(distinct(group)) as count
            SKIP $offset LIMIT $limit
            """.trimIndent()

        val parameters = mapOf(
            "groupsIds" to groupsIds.toList().toListOfString()
        )

        val result = neo4jClient
            .query(
                """
                $matchAuthorizedGroupsClause
                $pagingClause
                """
            )
            .bindAll(parameters)
            .fetch()
            .all()

        return if (limit == 0)
            Pair((result.first()["count"] as Long).toInt(), emptyList())
        else
            Pair(
                (result.first()["count"] as Long).toInt(),
                (result.firstOrNull()?.get("groups") as List<Map<String, Any>>).map {
                    Group(
                        id = (it["groupId"] as String).toUri(),
                        name = it["groupName"] as String
                    )
                }
            )
    }

    fun getGroupsForAdmin(groupsMemberships: Set<URI>, offset: Int, limit: Int): Pair<Int, List<Group>> {
        val matchAuthorizedGroupsClause =
            """
            MATCH (group:`$GROUP_TYPE`)-[:HAS_VALUE]->(p:Property { name:"$NGSILD_NAME_PROPERTY" })
            """.trimIndent()

        val pagingClause = if (limit == 0)
            """
            RETURN count(distinct(group)) as count
            """.trimIndent()
        else
            """
            WITH group, p
            ORDER BY group.id
            return 
                collect(distinct({
                    groupId:group.id, groupName:p.value
                })) as groups, 
                count(distinct(group)) as count
            SKIP $offset LIMIT $limit
            """.trimIndent()

        val result = neo4jClient
            .query(
                """
                $matchAuthorizedGroupsClause
                $pagingClause
                """
            )
            .fetch()
            .all()

        return if (limit == 0)
            Pair((result.first()["count"] as Long).toInt(), emptyList())
        else
            Pair(
                (result.first()["count"] as Long).toInt(),
                (result.firstOrNull()?.get("groups") as List<Map<String, Any>>).map {
                    val groupId = (it["groupId"] as String).toUri()
                    Group(
                        id = groupId,
                        isMember = groupsMemberships.contains(groupId),
                        name = it["groupName"] as String
                    )
                }
            )
    }
}
