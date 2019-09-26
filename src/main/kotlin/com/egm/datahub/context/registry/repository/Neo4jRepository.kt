package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.EntityStatements
import com.egm.datahub.context.registry.service.RelationshipStatements
import com.egm.datahub.context.registry.web.EntityCreationException
import com.egm.datahub.context.registry.web.NotExistingEntityException
import com.google.gson.GsonBuilder
import io.netty.util.internal.StringUtil
import org.neo4j.driver.v1.*
import org.neo4j.ogm.config.Configuration
import org.neo4j.ogm.response.model.NodeModel
import org.neo4j.ogm.session.SessionFactory
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class Neo4jRepository(
    neo4jProperties: Neo4jProperties
) {
    private val logger = LoggerFactory.getLogger(Neo4jRepository::class.java)
    private val gson = GsonBuilder().setPrettyPrinting().create()

    private lateinit var sessionFactory: SessionFactory

    init {
        val configuration = Configuration.Builder()
            .uri(neo4jProperties.uri)
            .credentials(neo4jProperties.username, neo4jProperties.password)
            .encryptionLevel(Config.EncryptionLevel.NONE.name)
            .build()
        sessionFactory = SessionFactory(configuration, "com.egm.datahub.context.registry")
    }

    fun createOrUpdateEntity(entityUrn: String, statements: Pair<EntityStatements, RelationshipStatements>): String {
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

    fun updateEntity(uri: String, statement: String) {
        if (!checkExistingUrn(uri)) {
            logger.info("not existing entity")
            throw NotExistingEntityException("not existing entity! ")
        }

        val updateResults = sessionFactory.openSession().query(statement, emptyMap<String, Any>()).queryResults()
        logger.info(gson.toJson((updateResults.first().get("a") as NodeModel).propertyList))
    }

    fun getNodesByURI(uri: String): List<Map<String, Any>> {
        val pattern = "{ uri: '$uri' }"
        return sessionFactory.openSession()
            .query("MATCH (n $pattern ) RETURN n", emptyMap<String, Any>(), true)
            .toList()
    }

    fun getEntitiesByLabel(label: String): List<Map<String, Any>> {
        return sessionFactory.openSession()
            .query("MATCH (s:$label) OPTIONAL MATCH (s:$label)-[r]->(o)  RETURN s, type(r), o", emptyMap<String, Any>(), true)
            .toList()
    }

    fun getEntitiesByLabelAndQuery(query: String, label: String): List<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return sessionFactory.openSession()
            .query(if (query.split("==")[1].startsWith("urn:")) "MATCH (s:$label)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o" else "MATCH (s:$label { $property : '$value' }) OPTIONAL MATCH (s:$label { $property : '$value' })-[r]->(o) RETURN s,type(r),o", emptyMap<String, Any>())
            .toList()
    }

    fun getEntitiesByQuery(query: String): List<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return sessionFactory.openSession()
            .query(if (query.split("==")[1].startsWith("urn:")) "MATCH (s)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o" else "MATCH (s { $property : '$value' }) OPTIONAL MATCH (s { $property : '$value' })-[r]->(o)  RETURN s,type(r),o", HashMap<String, Any>())
            .toList()
    }

    fun getEntities(query: String, label: String): List<Map<String, Any>> {
        if (!StringUtil.isNullOrEmpty(query) && !StringUtil.isNullOrEmpty(label)) {
            return getEntitiesByLabelAndQuery(query, label)
        } else if (StringUtil.isNullOrEmpty(query) && !StringUtil.isNullOrEmpty(label)) {
            return getEntitiesByLabel(label)
        } else if (StringUtil.isNullOrEmpty(label) && !StringUtil.isNullOrEmpty(query)) {
            return getEntitiesByQuery(query)
        }
        return emptyList()
    }

    fun checkExistingUrn(entityUrn: String): Boolean {
        return getNodesByURI(entityUrn).isNotEmpty()
    }

    fun convert(value: Value): Any {
        when (value.type().name()) {
            "PATH" -> return value.asList(this::convert)
            "NODE" -> return value.asMap()
            "RELATIONSHIP" -> return value.asMap()
        }
        return value.asObject()
    }

    fun addNamespaceDefinition(url: String, prefix: String) {
        val addNamespacesStatement = "CREATE (:NamespacePrefixDefinition { `$url`: '$prefix'})"
        sessionFactory.openSession().query(addNamespacesStatement, emptyMap<String, Any>())
    }
}
