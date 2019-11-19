package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.EntityStatements
import com.egm.datahub.context.registry.service.RelationshipStatements
import com.egm.datahub.context.registry.web.InternalErrorException
import com.egm.datahub.context.registry.web.ResourceNotFoundException
import org.neo4j.driver.v1.Config
import org.neo4j.ogm.config.Configuration
import org.neo4j.ogm.session.SessionFactory
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class Neo4jRepository(
    private val neo4jProperties: Neo4jProperties

) {
    private val logger = LoggerFactory.getLogger(Neo4jRepository::class.java)
    private lateinit var sessionFactory: SessionFactory

    init {
        val configuration = Configuration.Builder()
            .uri(neo4jProperties.uri)
            .credentials(neo4jProperties.username, neo4jProperties.password)
            .encryptionLevel(Config.EncryptionLevel.NONE.name)
            .build()
        sessionFactory = SessionFactory(configuration, "com.egm.datahub.context.registry.model.neo4j")
    }

    fun createEntity(entityUrn: String, entityStatements: EntityStatements, relationshipStatements: RelationshipStatements): String {
        val session = sessionFactory.openSession()
        val tx = session.beginTransaction()
        try {
            // This constraint ensures that each profileId is unique per user node

            // insert entities first
            entityStatements.forEach {
                logger.info("Creating entity : $it")
                session.query(it, emptyMap<String, Any>())
            }

            // insert relationships second
            relationshipStatements.forEach {
                logger.info("Creating relation : $it")
                session.query(it, emptyMap<String, Any>())
            }

            // UPDATE RELATIONSHIP MATERIALIZED NODE with same URI

            tx.commit()
        } catch (ex: Exception) {
            // The constraint is already created or the database is not available
            logger.error("Error while persisting entity $entityUrn", ex)
            tx.rollback()
            throw InternalErrorException("Error while persisting entity")
        } finally {
            tx.close()
        }
        return entityUrn
    }
    fun updateEntity(update: String): Map<String, Any> {
        val nodes: List<Map<String, Any>> = sessionFactory.openSession().query(update, emptyMap<String, Any>()).toMutableList()
        return if (nodes.isEmpty())
            emptyMap()
        else nodes.first()
    }

    fun updateEntity(query: String, uri: String): Map<String, Any> {
        if (!checkExistingUrn(uri)) {
            logger.info("not existing entity")
            throw ResourceNotFoundException("not existing entity!")
        }

        val nodes: List<Map<String, Any>> = sessionFactory.openSession().query(query, emptyMap<String, Any>()).toMutableList()
        return if (nodes.isEmpty())
            emptyMap()
        else nodes.first()
    }

    fun deleteEntity(uri: String): Pair<Int, Int> {
        /**
         * 1. delete the entity node
         * 2. delete the properties persisted as nodes
         * 3. delete the relationships
         * 4. delete the relationships on relationships persisted as nodes
         */
        val query = """
            MATCH (n { uri: '$uri' }) 
            OPTIONAL MATCH (n)-[rp:ngsild__hasValue]->(p)
            OPTIONAL MATCH (n)-[rr]-() WHERE type(rr) <> 'ngsild__hasValue'
            OPTIONAL MATCH (nr { uri: rr.uri })
            DETACH DELETE n,rp,p,rr,nr
        """.trimIndent()

        val queryStatistics = sessionFactory.openSession().query(query, emptyMap<String, Any>()).queryStatistics()
        logger.debug("Deleted entity $uri : deleted ${queryStatistics.nodesDeleted} nodes, ${queryStatistics.relationshipsDeleted} relations")
        return Pair(queryStatistics.nodesDeleted, queryStatistics.relationshipsDeleted)
    }

    fun getNodeByURI(uri: String): Map<String, Any> {
        val query = """
            MATCH (n { uri: '$uri' }) 
            RETURN n
        """.trimIndent()
        val nodes: List<Map<String, Any>> = sessionFactory.openSession().query(query, HashMap<String, Any>(), true).toMutableList()

        return if (nodes.isEmpty())
            emptyMap()
        else nodes.first()
    }

    fun getRelationshipByURI(uri: String): List<Map<String, Any>> {
        val query = """
            MATCH (n { uri: '$uri' })-[r]->(t) 
            WHERE NOT (n)-[r:ngsild__hasValue]->(t) 
            RETURN n,type(r) as rel,t,r
        """.trimIndent()
        val nodes: List<Map<String, Any>> = sessionFactory.openSession().query(query, HashMap<String, Any>(), true).toMutableList()

        return if (nodes.isEmpty())
            emptyList()
        else nodes
    }

    fun getNestedPropertiesByURI(uri: String): List<Map<String, Any>> {
        val query = """
            MATCH (n { uri: '$uri' })-[r:ngsild__hasValue]->(t) 
            RETURN n,type(r) as rel,t,r
        """.trimIndent()
        val nodes: List<Map<String, Any>> = sessionFactory.openSession().query(query, HashMap<String, Any>(), true).toMutableList()

        return if (nodes.isEmpty())
            emptyList()
        else nodes
    }

    fun getEntitiesByLabel(label: String): MutableList<Map<String, Any>> {
        val query = """
            MATCH (n:$label) 
            RETURN n
        """.trimIndent()
        return sessionFactory.openSession().query(query, HashMap<String, Any>(), true).toMutableList()
    }

    fun getEntitiesByLabelAndQuery(query: List<String>, label: String?): MutableList<Map<String, Any>> {
        val queryCriteria = query
            .map {
                val splitted = it.split("==")
                Pair(splitted[0], splitted[1])
            }
            .partition {
                it.second.startsWith("urn:")
            }

        val propertiesFilter =
            if (queryCriteria.second.isNotEmpty())
                queryCriteria.second.joinToString(" AND ") {
                    "n.${it.first} = '${it.second}'"
                }
            else
                ""

        val relationshipsFilter =
            if (queryCriteria.first.isNotEmpty())
                queryCriteria.first.joinToString(" AND ") {
                    "(n)-[:${it.first}]->({ uri: '${it.second}' })"
                }
            else
                ""

        val matchClause =
            if (label.isNullOrEmpty())
                "MATCH (n)"
            else
                "MATCH (n:$label)"

        val finalQuery = """
            $matchClause
            WHERE
                $propertiesFilter
                ${if (propertiesFilter.isNotEmpty() && relationshipsFilter.isNotEmpty()) " AND " else ""}
                $relationshipsFilter
            RETURN n    
        """

        return sessionFactory.openSession().query(finalQuery,
            emptyMap<String, Any>(), true).toMutableList()
    }

    fun getEntities(query: List<String>, label: String): MutableList<Map<String, Any>> {
        return if (query.isEmpty() && label.isNotEmpty())
            getEntitiesByLabel(label)
        else
            getEntitiesByLabelAndQuery(query, label)
    }

    fun checkExistingUrn(entityUrn: String): Boolean {
        return getNodeByURI(entityUrn).isNotEmpty()
    }

    fun addNamespaceDefinition(url: String, prefix: String) {
        val addNamespacesStatement = "CREATE (:NamespacePrefixDefinition { `$url`: '$prefix'})"
        sessionFactory.openSession().query(addNamespacesStatement, emptyMap<String, Any>())
    }
}
