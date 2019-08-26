package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.JsonLDService
import com.egm.datahub.context.registry.web.AlreadyExistingEntityException
import org.neo4j.ogm.session.Session
import org.slf4j.LoggerFactory
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod
import org.springframework.http.client.support.BasicAuthenticationInterceptor
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestTemplate
import java.time.Instant
import java.time.format.DateTimeFormatter


@Component
class Neo4jRepository(
        private val ogmSession: Session,
        private val jsonLDService: JsonLDService,
        private val neo4jProperties : Neo4jProperties
) {
    private val headers = LinkedMultiValueMap<String, String>()
    private val logger = LoggerFactory.getLogger(Neo4jRepository::class.java)
    fun createEntity(jsonld: String): Long {
        val entityUrn = jsonLDService.parsePayload(jsonld)
        if (checkExistingUrn(entityUrn)) {
            throw AlreadyExistingEntityException("already existing entity $entityUrn")
        }
        val importStatement = """
            CALL semantics.importRDFSnippet(
                '$jsonld',
                'JSON-LD',
                { handleVocabUris: 'SHORTEN', typesToLabels: true, commitSize: 500 }
            )
        """.trimIndent()

        val queryResults = ogmSession.query(importStatement, emptyMap<String, Any>()).queryResults()
        addCreatedAtProperty(entityUrn)
        return queryResults.first()["triplesLoaded"] as Long
    }

    fun updateEntity(jsonld: String): Long {
        val entityUrn = jsonLDService.parsePayload(jsonld)
        if (!checkExistingUrn(entityUrn)) {
            logger.info("not existing entity")
            createEntity(entityUrn)
        }
        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        val update: String = "MATCH(o {uri:$entityUrn}) SET o.modifiedAt = $timestamp"

        val queryResults = ogmSession.query(update, emptyMap<String, Any>()).queryResults()
        return queryResults.first()["triplesLoaded"] as Long
    }

    fun addCreatedAtProperty(entityUrn: String): String {
        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        val insert: String = "MATCH(o {uri:$entityUrn}) SET o.createdAt = $timestamp"
        val result = ogmSession.query(insert, emptyMap<String, Any>())
        return result.queryResults().toString()
    }


    fun getByURI(uri: String): List<Map<String, Any>> {
        val pattern = "{ uri: '$uri' }"
        val matchQuery = """
            MATCH (n $pattern ) RETURN n
        """.trimIndent()
        val result = ogmSession.query(matchQuery, emptyMap<String, Any>())
        return result.queryResults().toList()
    }


    fun getEntitiesByLabel(label: String): List<Map<String, Any>> {
        return performQuery("{ \"cypher\" : \"MATCH (o:$label) OPTIONAL MATCH (o:$label)-[r]-(s )  RETURN o , r \" , \"format\": \"JSON-LD\" }")
    }

    fun getEntitiesByLabelAndQuery(query: String, label: String): List<Map<String, Any>> {
        var q = ""
        //RELATION
        if (query.split("==")[1].startsWith("urn:")) {
            val rel = query.split("==")[0]
            val node = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"MATCH (o:$label)-[r:$rel]-(s { uri : \\\"$node\\\" })  RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        } else {
            //PROPERTY
            val property = query.split("==")[0]
            val value = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"MATCH (o:$label { $property : \\\"$value\\\" }) OPTIONAL MATCH (o:$label { $property : \\\"$value\\\" })-[r]-(s) RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        }
    }

    fun getEntitiesByQuery(query: String): List<Map<String, Any>> {
        var q = ""
        if (query.split("==")[1].startsWith("urn:")) {
            val rel = query.split("==")[0]
            val node = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"MATCH (o)-[r:$rel]-(s { uri : \\\"$node\\\" })  RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        } else {
            val property = query.split("==")[0]
            val value = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"MATCH (o { $property : \\\"$value\\\" }) OPTIONAL MATCH (o { $property : \\\"$value\\\" })-[r]-(s)  RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        }
    }

    fun performQuery(cypher: String): List<Map<String, Any>> {
        headers.add("Content-Type", "application/json")
        headers.add("Accept", "application/ld+json")
        val request = HttpEntity<String>(cypher, headers)
        try {
            val restTemplate = RestTemplate()
            restTemplate.getInterceptors().add(
                    BasicAuthenticationInterceptor(neo4jProperties.username, neo4jProperties.password))
            val response = restTemplate.exchange(
                    "http://docker:7474/rdf/cypheronrdf",
                    HttpMethod.POST,
                    request,
                    object : ParameterizedTypeReference<List<Map<String, Any>>>() {

                    }).body.orEmpty()
            return response
        } catch (e: Exception) {
            logger.error(e.message)
        }
        return emptyList()
    }

    fun checkExistingUrn(entityUrn: String): Boolean {
        if (getByURI(entityUrn).isNotEmpty()) return true else return false
    }

}
