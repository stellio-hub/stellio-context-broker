package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import org.neo4j.ogm.session.Session
import org.springframework.stereotype.Component

@Component
class Neo4jAuthorizationRepository(
    private val session: Session
) {

    data class AvailableRightsForEntity(
        val targetEntityId: String,
        val right: Relationship? = null,
        val grpRight: Relationship? = null
    )

    fun getAvailableRightsForEntities(userId: String, entitiesId: List<String>): List<AvailableRightsForEntity> {
        val query = """
            MATCH (userEntity:Entity {id : ${'$'}userId}), (entity:Entity)
            WHERE entity.id IN ${'$'}entitiesId
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->(entity:Entity)
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)-[:IS_MEMBER_OF]->(:Entity)-[:HAS_OBJECT]->(grpRight:Attribute:Relationship)-[]->(entity:Entity)
            RETURN entity.id as id, right, grpRight
        """.trimIndent()

        val parameters = mapOf(
            "userId" to userId,
            "entitiesId" to entitiesId
        )

        return session.query(query, parameters).map {
            AvailableRightsForEntity(it["id"] as String, it["right"] as Relationship?, it["grpRight"] as Relationship?)
        }
    }

    fun getUserRoles(userId: String): List<String> {
        val query = """
            MATCH (userEntity:Entity { id: ${'$'}userId })
            OPTIONAL MATCH  (userEntity)-[:HAS_VALUE]->(p:Property { name: "https://ontology.eglobalmark.com/authorization#roles" })
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]-(r:Attribute:Relationship)-[:IS_MEMBER_OF]->(group:Entity)-[:HAS_VALUE]->
                (pgroup: Property {name: "https://ontology.eglobalmark.com/authorization#roles"})
            RETURN p, pgroup
        """.trimIndent()

        val parameters = mapOf(
            "userId" to userId
        )

        val result = session.query(query, parameters)

        if (result.toList().isEmpty()) {
            return listOf()
        }

        return result
            .flatMap {
                listOf(
                    (it["p"] as Property?)?.value as List<String>?,
                    (it["pgroup"] as Property?)?.value as List<String>?
                ).filterNotNull().flatten()
            }
    }
}
