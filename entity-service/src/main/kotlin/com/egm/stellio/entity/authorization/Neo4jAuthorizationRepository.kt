package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.EGM_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.R_CAN_ADMIN
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.SERVICE_ACCOUNT_ID
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import org.neo4j.ogm.session.Session
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class Neo4jAuthorizationRepository(
    private val session: Session
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun filterEntitiesUserHasOneOfGivenRights(
        userId: String,
        entitiesId: List<String>,
        rights: Set<String>
    ): List<String> {
        val query =
            """
            MATCH (userEntity:Entity), (entity:Entity)
            WHERE entity.id IN ${'$'}entitiesId
            AND (userEntity.id = ${'$'}userId 
                OR (userEntity)-[:HAS_VALUE]->(:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId }))
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
            "userId" to userId,
            "entitiesId" to entitiesId,
            "rights" to rights
        )

        return session.query(query, parameters).map {
            it["id"] as String
        }
    }

    fun getUserRoles(userId: String): Set<String> {
        val query =
            """
            MATCH (userEntity:Entity { id: ${'$'}userId })
            OPTIONAL MATCH (userEntity)-[:HAS_VALUE]->(p:Property { name:"$EGM_ROLES" })
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]-(r:Attribute:Relationship)-
                [:isMemberOf]->(group:Entity)-[:HAS_VALUE]->(pgroup:Property { name: "$EGM_ROLES" })
            RETURN p, pgroup
            UNION
            MATCH (client:Entity)-[:HAS_VALUE]->(sid:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId })
            OPTIONAL MATCH (client)-[:HAS_VALUE]->(p:Property { name:"$EGM_ROLES" })
            OPTIONAL MATCH (client)-[:HAS_OBJECT]-(r:Attribute:Relationship)-
                [:isMemberOf]->(group:Entity)-[:HAS_VALUE]->(pgroup:Property { name: "$EGM_ROLES" })
            RETURN p, pgroup
            """.trimIndent()

        val parameters = mapOf(
            "userId" to userId
        )

        val result = session.query(query, parameters)

        if (result.toSet().isEmpty()) {
            return emptySet()
        }

        return result
            .flatMap {
                listOfNotNull(
                    propertyToListOfRoles(it["p"]),
                    propertyToListOfRoles(it["pgroup"])
                ).flatten()
            }
            .toSet()
    }

    private fun propertyToListOfRoles(property: Any?): List<String> {
        val rolesProperty = property as Property?

        return when (rolesProperty?.value) {
            is String -> listOf(rolesProperty.value as String)
            is List<*> -> rolesProperty.value as List<String>
            else -> {
                logger.warn("Unknown value type for roles property: ${rolesProperty?.value}")
                emptyList()
            }
        }
    }

    fun createAdminLinks(userId: String, relationships: List<Relationship>, entitiesId: List<String>): List<String> {
        val query =
            """
            UNWIND ${'$'}relPropsAndTargets AS relPropAndTarget
            MATCH (user:Entity), (target:Entity { id: relPropAndTarget.second })
            WHERE (user.id = ${'$'}userId 
                OR (user)-[:HAS_VALUE]->(:Property { name: "$SERVICE_ACCOUNT_ID", value: ${'$'}userId }))
            CREATE (user)-[:HAS_OBJECT]->(r:Attribute:Relationship:`$R_CAN_ADMIN`)-[:rCanAdmin]->(target)
            SET r = relPropAndTarget.first
            RETURN r.id as id
        """

        val parameters = mapOf(
            "relPropsAndTargets" to relationships.map { it.nodeProperties() }.zip(entitiesId),
            "userId" to userId
        )

        return session.query(query, parameters).map {
            it["id"] as String
        }
    }
}
