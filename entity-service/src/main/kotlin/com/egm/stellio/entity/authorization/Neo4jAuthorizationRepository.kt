package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.model.Property
import org.neo4j.ogm.session.Session
import org.springframework.stereotype.Component

@Component
class Neo4jAuthorizationRepository(
    private val session: Session
) {

    fun filterEntitiesUserHasOneOfGivenRights(
        userId: String,
        entitiesId: List<String>,
        rights: Set<String>
    ): List<String> {
        val query = """
            MATCH (userEntity:Entity {id : ${'$'}userId}), (entity:Entity)
            WHERE entity.id IN ${'$'}entitiesId
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]->(right:Attribute:Relationship)-[]->(entity:Entity)
            OPTIONAL MATCH (userEntity)-[:HAS_OBJECT]->(:Attribute:Relationship)-[:IS_MEMBER_OF]->(:Entity)-[:HAS_OBJECT]->(grpRight:Attribute:Relationship)-[]->(entity:Entity)
            WHERE size([label IN labels(right) WHERE label IN ${'$'}rights | 1]) > 0 AND
                    size([label IN labels(grpRight) WHERE label IN ${'$'}rights | 1]) > 0
            RETURN entity.id as id, count(right) AS nbOfRights, count(grpRight) AS nbOfGrpRights
        """.trimIndent()

        val parameters = mapOf(
            "userId" to userId,
            "entitiesId" to entitiesId,
            "rights" to rights
        )

        return session.query(query, parameters).filter {
            it["nbOfRights"] as Long + it["nbOfGrpRights"] as Long > 0
        }.map {
            it["id"] as String
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
            return emptyList()
        }

        return result
            .flatMap {
                listOfNotNull(
                    (it["p"] as Property?)?.value as List<String>?,
                    (it["pgroup"] as Property?)?.value as List<String>?
                ).flatten()
            }
    }
}
