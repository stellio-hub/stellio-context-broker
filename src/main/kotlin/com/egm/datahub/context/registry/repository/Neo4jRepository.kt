package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.EntityStatements
import com.egm.datahub.context.registry.service.RelationshipStatements
import com.egm.datahub.context.registry.web.EntityCreationException
import com.egm.datahub.context.registry.web.NotExistingEntityException
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
            throw EntityCreationException("Something went wrong when creating entity")
        } finally {
            tx.close()
        }

        return entityUrn
    }

    fun updateEntity(query: String, uri: String) {
        if (!checkExistingUrn(uri)) {
            logger.info("not existing entity")
            throw NotExistingEntityException("not existing entity! ")
        }

        sessionFactory.openSession().query(query, emptyMap<String, Any>()).queryResults()
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
            WHERE NOT (n)-[r:ngsild__hasObject]->(t) 
            RETURN n,type(r) as rel,t,r
        """.trimIndent()
        val nodes: List<Map<String, Any>> = sessionFactory.openSession().query(query, HashMap<String, Any>(), true).toMutableList()

        return if (nodes.isEmpty())
            emptyList()
        else nodes
    }

    fun getNestedPropertiesByURI(uri: String): List<Map<String, Any>> {
        val query = """
            MATCH (n { uri: '$uri' })-[r:ngsild__hasObject]->(t) 
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
