package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.CLIENT_LABEL
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.EGM_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.SERVICE_ACCOUNT_ID
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_LABEL
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.shared.util.JsonLdUtils.EGM_SPECIFIC_ACCESS_POLICY
import com.egm.stellio.shared.util.toListOfString
import com.egm.stellio.shared.util.toUri
import org.springframework.data.neo4j.core.Neo4jClient
import org.springframework.stereotype.Component
import java.net.URI

@Component
class Neo4jAuthorizationRepository(
    private val neo4jClient: Neo4jClient
) {

    fun filterEntitiesUserHasOneOfGivenRights(
        userId: URI,
        entitiesId: List<URI>,
        rights: Set<String>
    ): List<URI> {
        val query =
            """
            MATCH (userEntity:Entity)
            WHERE (userEntity.id = ${'$'}userId
                OR (userEntity)-[:HAS_VALUE]->(:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId }))
            WITH userEntity 
            MATCH (entity:Entity)
            WHERE entity.id IN ${'$'}entitiesId
            WITH userEntity, entity
            MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->(entity:Entity)
            WHERE size([label IN labels(right) WHERE label IN ${'$'}rights]) > 0
            return entity.id as id
            UNION
            MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)-
                [:isMemberOf]->(:Entity)-[:HAS_OBJECT]-(grpRight:Attribute:Relationship)-[]->(entity:Entity)
            WHERE size([label IN labels(grpRight) WHERE label IN ${'$'}rights]) > 0
            return entity.id as id
            """.trimIndent()

        val parameters = mapOf(
            "userId" to userId.toString(),
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
            MATCH (entity)-[:HAS_VALUE]->(p:Property { name: "$EGM_SPECIFIC_ACCESS_POLICY" })
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

    fun getUserRoles(userId: URI): Set<String> {
        val query =
            """
            MATCH (userEntity:Entity { id: ${'$'}userId })
            OPTIONAL MATCH (userEntity)-[:HAS_VALUE]->(p:Property { name:"$EGM_ROLES" })
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]-(r:Attribute:Relationship)-
                [:isMemberOf]->(group:Entity)-[:HAS_VALUE]->(pgroup:Property { name: "$EGM_ROLES" })
            RETURN apoc.coll.union(collect(p.value), collect(pgroup.value)) as roles
            UNION
            MATCH (client:Entity)-[:HAS_VALUE]->(sid:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId })
            OPTIONAL MATCH (client)-[:HAS_VALUE]->(p:Property { name:"$EGM_ROLES" })
            OPTIONAL MATCH (client)-[:HAS_OBJECT]-(r:Attribute:Relationship)-
                [:isMemberOf]->(group:Entity)-[:HAS_VALUE]->(pgroup:Property { name: "$EGM_ROLES" })
            RETURN apoc.coll.union(collect(p.value), collect(pgroup.value)) as roles
            """.trimIndent()

        val parameters = mapOf(
            "userId" to userId.toString()
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

    fun createAdminLinks(userId: URI, relationships: List<Relationship>, entitiesId: List<URI>): List<URI> {
        val query =
            """
            CALL {
                MATCH (user:Entity:`$USER_LABEL`)
                WHERE user.id = ${'$'}userId
                RETURN user
                UNION
                MATCH (user:Entity:`$CLIENT_LABEL`)
                WHERE (user)-[:HAS_VALUE]->(:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId })
                RETURN user
            }
            WITH user
            UNWIND ${'$'}relPropsAndTargets AS relPropAndTarget
            MATCH (target:Entity { id: relPropAndTarget.targetEntityId })
            CREATE (user)-[:HAS_OBJECT]->(r:Attribute:Relationship:`$R_CAN_ADMIN`)-[:rCanAdmin]->(target)
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
            "userId" to userId.toString()
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
            MATCH (subject:Entity { id: ${'$'}entityId })-[:HAS_OBJECT]-(relNode)
                    -[]->(target:Entity { id: ${'$'}targetId })
            DETACH DELETE relNode
            """.trimIndent()

        val parameters = mapOf(
            "entityId" to subjectId,
            "targetId" to targetId
        )

        return session.query(matchQuery, parameters).queryStatistics().nodesDeleted
    }
}
