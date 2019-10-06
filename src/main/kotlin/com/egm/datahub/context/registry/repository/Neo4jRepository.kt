package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.EntityStatements
import com.egm.datahub.context.registry.service.RelationshipStatements
import com.egm.datahub.context.registry.web.EntityCreationException
import com.egm.datahub.context.registry.web.NotExistingEntityException
import com.google.gson.GsonBuilder
import io.netty.util.internal.StringUtil
import org.neo4j.driver.v1.Config
import org.neo4j.ogm.config.Configuration
import org.neo4j.ogm.response.model.NodeModel
import org.neo4j.ogm.session.SessionFactory
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class Neo4jRepository(
    private val neo4jProperties: Neo4jProperties

) {
    private val gson = GsonBuilder().setPrettyPrinting().create()
    private val logger = LoggerFactory.getLogger(Neo4jRepository::class.java)
    private lateinit var sessionFactory: SessionFactory

    init {
        val configuration = Configuration.Builder()
            .uri(neo4jProperties.uri)
            .credentials(neo4jProperties.username, neo4jProperties.password)
            .encryptionLevel(Config.EncryptionLevel.NONE.name)
            .build()
        sessionFactory = SessionFactory(configuration, "com.egm.datahub.context.registry")
    }

    fun createEntity(entityUrn: String, statements: Pair<EntityStatements, RelationshipStatements>): String {
        val session = sessionFactory.openSession()
        val tx = session.beginTransaction()
        try {
            // This constraint ensures that each profileId is unique per user node

            // insert entities first
            statements.first.forEach {
                logger.info("Creating entity : $it")
                session.query(it, emptyMap<String, Any>())
            }

            // insert relationships second
            statements.second.forEach {
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

        val updateResults = sessionFactory.openSession().query(query, emptyMap<String, Any>()).queryResults()
        logger.info(gson.toJson((updateResults.first().get("a") as NodeModel).propertyList))
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
            MATCH (s:$label) 
            OPTIONAL MATCH (s:$label)-[r]->(o)
            RETURN s, type(r), o
        """.trimIndent()
        return sessionFactory.openSession().query(query, HashMap<String, Any>(), true).toMutableList()
    }

    fun getEntitiesByLabelAndQuery(query: String, label: String): MutableList<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return sessionFactory.openSession().query(
            if (query.split("==")[1].startsWith("urn:"))
                "MATCH (s:$label)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o"
            else
                "MATCH (s:$label { $property : '$value' }) OPTIONAL MATCH (s:$label { $property : '$value' })-[r]->(o) RETURN s,type(r),o",
            HashMap<String, Any>(), true).toMutableList()
    }

    fun getEntitiesByQuery(query: String): MutableList<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return sessionFactory.openSession().query(
            if (query.split("==")[1].startsWith("urn:"))
                "MATCH (s)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o"
            else
                "MATCH (s { $property : '$value' }) OPTIONAL MATCH (s { $property : '$value' })-[r]->(o)  RETURN s,type(r),o",
            HashMap<String, Any>(), true).toMutableList()
    }

    fun getEntities(query: String, label: String): MutableList<Map<String, Any>> {
        if (!StringUtil.isNullOrEmpty(query) && !StringUtil.isNullOrEmpty(label)) {
            return getEntitiesByLabelAndQuery(query, label)
        } else if (StringUtil.isNullOrEmpty(query) && !StringUtil.isNullOrEmpty(label)) {
            return getEntitiesByLabel(label)
        } else if (StringUtil.isNullOrEmpty(label) && !StringUtil.isNullOrEmpty(query)) {
            return getEntitiesByQuery(query)
        }
        return arrayListOf()
    }

    fun checkExistingUrn(entityUrn: String): Boolean {
        return getNodeByURI(entityUrn).isNotEmpty()
    }

    fun addNamespaceDefinition(url: String, prefix: String) {
        val addNamespacesStatement = "CREATE (:NamespacePrefixDefinition { `$url`: '$prefix'})"
        sessionFactory.openSession().query(addNamespacesStatement, emptyMap<String, Any>())
    }

}
