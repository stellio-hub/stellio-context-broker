package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.EntityStatements
import com.egm.datahub.context.registry.service.RelationshipStatements
import com.egm.datahub.context.registry.web.EntityCreationException
import com.egm.datahub.context.registry.web.NotExistingEntityException
import com.google.gson.GsonBuilder
import io.netty.util.internal.StringUtil
import org.neo4j.driver.v1.*
import org.neo4j.ogm.response.model.NodeModel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class Neo4jRepository(
    neo4jProperties: Neo4jProperties
) {
    private val logger = LoggerFactory.getLogger(Neo4jRepository::class.java)
    private val gson = GsonBuilder().setPrettyPrinting().create()

    private lateinit var driver: Driver

    init {
        val config = Config.build()
            .withoutEncryption()
            .toConfig()
        driver = GraphDatabase.driver(neo4jProperties.uri, AuthTokens.basic(neo4jProperties.username, neo4jProperties.password), config)
    }

    fun createOrUpdateEntity(entityUrn: String, statements: Pair<EntityStatements, RelationshipStatements>): String {
        val session = driver.session()
        val tx = session.beginTransaction()
        try {
            // This constraint ensures that each profileId is unique per user node

            // insert entities first
            statements.first.forEach {
                logger.info("Creating entity : $it")
                tx.run(it, emptyMap<String, Any>())
            }

            // insert relationships second
            statements.second.forEach {
                logger.info("Creating relation : $it")
                tx.run(it, emptyMap<String, Any>())
            }

            // UPDATE RELATIONSHIP MATERIALIZED NODE with same URI

            tx.success()
        } catch (ex: Exception) {
            // The constraint is already created or the database is not available
            ex.printStackTrace()
            tx.failure()
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

        val updateResults = driver.session().run(statement, emptyMap<String, Any>()).list()
        logger.info(gson.toJson((updateResults.first().get("a") as NodeModel).propertyList))
    }

    fun getNodesByURI(uri: String): List<Map<String, Any>> {
        val pattern = "{ uri: '$uri' }"
        return driver.session().run("MATCH (n $pattern ) RETURN n", HashMap<String, Any>())
            .list().map { it.asMap() }
    }

    fun getEntitiesByLabel(label: String): List<Map<String, Any>> {
        return driver.session().run("MATCH (s:$label) OPTIONAL MATCH (s:$label)-[r]->(o)  RETURN s, type(r), o", HashMap<String, Any>())
            .list().map { it.asMap() }
    }

    fun getEntitiesByLabelAndQuery(query: String, label: String): List<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return driver.session().run(if (query.split("==")[1].startsWith("urn:")) "MATCH (s:$label)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o" else "MATCH (s:$label { $property : '$value' }) OPTIONAL MATCH (s:$label { $property : '$value' })-[r]->(o) RETURN s,type(r),o", HashMap<String, Any>())
            .list().map { it.asMap() }
    }

    fun getEntitiesByQuery(query: String): List<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return driver.session().run(if (query.split("==")[1].startsWith("urn:")) "MATCH (s)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o" else "MATCH (s { $property : '$value' }) OPTIONAL MATCH (s { $property : '$value' })-[r]->(o)  RETURN s,type(r),o", HashMap<String, Any>())
            .list().map { it.asMap() }
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
        driver.session().run(addNamespacesStatement, emptyMap<String, Any>())
    }
}
