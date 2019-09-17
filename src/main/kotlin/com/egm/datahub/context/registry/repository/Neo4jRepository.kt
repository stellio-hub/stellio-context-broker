package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.EntityStatements
import com.egm.datahub.context.registry.service.JsonLDService
import com.egm.datahub.context.registry.service.RelationshipStatements
import com.egm.datahub.context.registry.web.EntityCreationException
import com.egm.datahub.context.registry.web.NotExistingEntityException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.netty.util.internal.StringUtil
import org.neo4j.ogm.session.Session
import org.slf4j.LoggerFactory
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import org.springframework.http.client.support.BasicAuthenticationInterceptor
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestTemplate
import java.time.Instant
import java.time.format.DateTimeFormatter

@Component
@Transactional
class Neo4jRepository(
    private val ogmSession: Session,
    private val jsonLDService: JsonLDService,
    private val neo4jProperties: Neo4jProperties
) {
    private val jackson = jacksonObjectMapper()
    private val logger = LoggerFactory.getLogger(Neo4jRepository::class.java)

    fun createEntity(entityUrn: String, statements: Pair<EntityStatements, RelationshipStatements>): String {
        val tx = ogmSession.transaction
        try {
            // This constraint ensures that each profileId is unique per user node

            // insert entities first
            statements.first.forEach {
                logger.info(it)
                ogmSession.query(it, emptyMap<String, Any>()).queryResults()
            }

            // insert relationships second
            statements.second.forEach {
                logger.info(it)
                ogmSession.query(it, emptyMap<String, Any>()).queryResults()
            }

            // UPDATE RELATIONSHIP MATERIALIZED NODE with same URI

            tx.commit()
            tx.close()
        } catch (ex: Exception) {
            // The constraint is already created or the database is not available
            ex.printStackTrace()
            tx.rollback()
            throw EntityCreationException("Something went wrong when creating entity")
        }

        return entityUrn
    }

    fun updateEntity(payload: String, uri: String, attr: String) {
        if (!checkExistingUrn(uri)) {
            logger.info("not existing entity")
            throw NotExistingEntityException("not existing entity! ")
        }

        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).toString()
        val value = this.jsonLDService.getValueOfProperty(payload, attr, "foaf")
        val update = "MATCH(o {uri : '$uri'}) SET o.modifiedAt = '$timestamp' , o.$attr = '$value'"

        ogmSession.query(update, emptyMap<String, Any>()).queryResults()
    }

    fun getNodesByURI(uri: String): MutableList<Map<String, Any>> {
        val pattern = "{ uri: '$uri' }"
        return ogmSession.query("MATCH (n $pattern ) RETURN n", HashMap<String, Any>()).toMutableList()
    }

    fun getEntitiesByLabel(label: String): MutableList<Map<String, Any>> {
        return ogmSession.query("MATCH (s:$label) OPTIONAL MATCH (s:$label)-[r]->(o)  RETURN s, type(r), o", HashMap<String, Any>()).toMutableList() //
    }

    fun getEntitiesByLabelAndQuery(query: String, label: String): MutableList<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return ogmSession.query(if (query.split("==")[1].startsWith("urn:")) "MATCH (s:$label)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o" else "MATCH (s:$label { $property : '$value' }) OPTIONAL MATCH (s:$label { $property : '$value' })-[r]->(o) RETURN s,type(r),o", HashMap<String, Any>()).toMutableList()
    }

    fun getEntitiesByQuery(query: String): MutableList<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        return ogmSession.query(if (query.split("==")[1].startsWith("urn:")) "MATCH (s)-[r:$property]->(o { uri : '$value' })  RETURN s,type(r),o" else "MATCH (s { $property : '$value' }) OPTIONAL MATCH (s { $property : '$value' })-[r]->(o)  RETURN s,type(r),o", HashMap<String, Any>()).toMutableList()
    }

    fun getEntities(query: String, label: String): MutableList<Map<String, Any>> {
        if (!StringUtil.isNullOrEmpty(query) && !StringUtil.isNullOrEmpty(label)) {
            return getEntitiesByLabelAndQuery(query, label)
        } else if (StringUtil.isNullOrEmpty(query) && !StringUtil.isNullOrEmpty(label)) {
            return getEntitiesByLabel(label)
        } else if (StringUtil.isNullOrEmpty(label) && !StringUtil.isNullOrEmpty(query)) {
            return getEntitiesByQuery(query)
        }
        return ArrayList<Map<String, Any>>()
    }

    fun performQuery(payload: Map<String, Any>): List<Map<String, Any>> {
        val headers = LinkedMultiValueMap<String, String>()
        headers.add("Content-Type", "application/json")
        headers.add("Accept", "application/ld+json")

        val cypher = jackson.writeValueAsString(payload)
        val request = HttpEntity(cypher, headers)
        try {
            val restTemplate = RestTemplate()
            restTemplate.interceptors.add(
                    BasicAuthenticationInterceptor(neo4jProperties.username, neo4jProperties.password))
            return restTemplate.exchange(
                    neo4jProperties.nsmntx,
                    HttpMethod.POST,
                    request,
                    object : ParameterizedTypeReference<List<Map<String, Any>>>() {
                    }).body.orEmpty()
        } catch (e: Exception) {
            logger.error(e.message)
        }
        return emptyList()
    }
    fun checkExistingUrn(entityUrn: String): Boolean {
        return getNodesByURI(entityUrn).isNotEmpty()
    }
    fun addNamespaceDefinition(url: String, prefix: String) {
        val addNamespacesStatement = "CREATE (:NamespacePrefixDefinition { `$url`: '$prefix'})"
        val queryResults = ogmSession.query(addNamespacesStatement, emptyMap<String, Any>()).queryResults()
    }
}
