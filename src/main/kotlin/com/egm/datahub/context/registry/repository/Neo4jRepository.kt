package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.parser.NgsiLdParser
import com.egm.datahub.context.registry.service.JsonLDService
import com.egm.datahub.context.registry.web.NotExistingEntityException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
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
    private val gson = GsonBuilder().setPrettyPrinting().create()

    private var namespacesMapping: Map<String, List<String>> = mapOf(
        "diat" to listOf("Beekeeper", "BeeHive", "Door", "DoorNumber", "SmartDoor", "Sensor", "Observation", "ObservedBy", "ManagedBy"),
        "ngsild" to listOf("connectsTo", "hasObject", "observedAt", "createdAt", "modifiedAt", "datasetId", "instanceId", "GeoProperty", "Point", "Property", "Relationship"),
        "example" to listOf("availableSpotNumber", "OffStreetParking", "Vehicle", "isParked", "providedBy","Camera") // this is property of property in order to allow nested property we need to add it to model
    )
    fun createEntity(jsonld: String): Long {
        var entityMap: Map<String, Any> = gson.fromJson(jsonld, object : TypeToken<Map<String, Any>>() {}.type)
        val ngsiLd = NgsiLdParser.Builder().withContext(namespacesMapping).entity(entityMap).build()
        var x : Long= 0
        val tx = ogmSession.transaction
        try {
            // This constraint ensures that each profileId is unique per user node

            // insert entities first
            ngsiLd.entityStatements?.forEach {
                logger.info(it)
                val queryResults = ogmSession.query(it, emptyMap<String, Any>()).queryResults()
                logger.info(gson.toJson(queryResults))
                x++
            }

            // insert relationships second
            ngsiLd.relStatements?.forEach {
                logger.info(it)
                val queryResults = ogmSession.query(it, emptyMap<String, Any>()).queryResults()
                logger.info(gson.toJson(queryResults))
                x++
            }

            // UPDATE RELATIONSHIP MATERIALIZED NODE with same URI

            tx.commit()
            tx.close()
        } catch (ex: Exception) {
            // The constraint is already created or the database is not available
            ex.printStackTrace()
            tx.rollback()
            return 0
        }
        return x
    }

    fun updateEntity(payload: String, uri: String, attr: String) {
        if (!checkExistingUrn(uri)) {
            logger.info("not existing entity")
            throw NotExistingEntityException("not existing entity! ")
        }

        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).toString()
        val value = this.jsonLDService.getValueOfProperty(payload, attr, "foaf")
        val update = "MATCH(o {uri : '$uri'}) SET o.modifiedAt = '$timestamp' , o.$attr = '$value'"

        val queryResults = ogmSession.query(update, emptyMap<String, Any>()).queryResults()
    }

    fun getNodesByURI(uri: String): List<Map<String, Any>> {
        val pattern = "{ uri: '$uri' }"
        val payload = mapOf("cypher" to "MATCH (n $pattern ) RETURN n", "format" to "JSON-LD")
        return performQuery(payload)
    }

    fun getRelationsByURI(uri: String): List<Map<String, Any>> {
        val pattern = "{ uri: '$uri' }"
        val payload = mapOf("cypher" to "MATCH ()-[r $pattern]->() return r", "format" to "JSON-LD")
        return performQuery(payload)
    }

    fun getEntitiesByLabel(label: String): List<Map<String, Any>> {
        val payload = mapOf("cypher" to "MATCH (o:$label) OPTIONAL MATCH (o:$label)-[r]->(s)  RETURN o , r", "format" to "JSON-LD")
        return performQuery(payload)
    }

    fun getEntitiesByLabelAndQuery(query: String, label: String): List<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        val payload = mapOf("cypher" to if (query.split("==")[1].startsWith("urn:")) "MATCH (o:$label)-[r:$property]->(s { uri : '$value' })  RETURN o,r" else "MATCH (o:$label { $property : '$value' }) OPTIONAL MATCH (o:$label { $property : '$value' })-[r]->(s) RETURN o,r", "format" to "JSON-LD")
        return performQuery(payload)
    }

    fun getEntitiesByQuery(query: String): List<Map<String, Any>> {
        val property = query.split("==")[0]
        val value = query.split("==")[1]
        val payload = mapOf("cypher" to if (query.split("==")[1].startsWith("urn:")) "MATCH (o)-[r:$property]->(s { uri : '$value' })  RETURN o,r" else "MATCH (o { $property : '$value' }) OPTIONAL MATCH (o { $property : '$value' })-[r]->(s)  RETURN o,r", "format" to "JSON-LD")
        return performQuery(payload)
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
