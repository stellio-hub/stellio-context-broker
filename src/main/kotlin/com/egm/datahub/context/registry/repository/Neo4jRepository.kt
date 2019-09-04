package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.config.properties.Neo4jProperties
import com.egm.datahub.context.registry.service.JsonLDService
import com.egm.datahub.context.registry.web.AlreadyExistingEntityException
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
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestTemplate
import java.time.Instant
import java.time.format.DateTimeFormatter

@Component
class Neo4jRepository(
    private val ogmSession: Session,
    private val jsonLDService: JsonLDService,
    private val neo4jProperties: Neo4jProperties
) {
    private val jackson = jacksonObjectMapper()
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
        //addCreatedAtProperty(entityUrn)
        println(queryResults.first()["extraInfo"])
        return queryResults.first()["triplesLoaded"] as Long
    }

    fun updateEntity(payload : String, uri: String, attr: String) {
        if (!checkExistingUrn(uri)) {
            logger.info("not existing entity")
            throw NotExistingEntityException("not existing entity! ")
        }

        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).toString()
        val value = this.jsonLDService.getValueOfProperty(payload, attr, "foaf")
        val update = "MATCH(o {uri : '$uri'}) SET o.modifiedAt = '$timestamp' , o.$attr = '$value'"

        val queryResults = ogmSession.query(update, emptyMap<String, Any>()).queryResults()
    }

    fun addCreatedAtProperty(entityUrn: String): String {
        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        val insert = "MATCH(o {uri: '$entityUrn'}) SET o.createdAt = $timestamp"
        val result = ogmSession.query(insert, emptyMap<String, Any>())
        return result.queryResults().toString()
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
                    BasicAuthenticationInterceptor("${neo4jProperties.username}", "${neo4jProperties.password}"))
            return restTemplate.exchange(
                    "${neo4jProperties.nsmntx}",
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
    fun addNamespaceDefinition(url : String, prefix : String){
        val addNamespacesStatement = "CREATE (:NamespacePrefixDefinition { `$url`: '$prefix'})"
        val queryResults = ogmSession.query(addNamespacesStatement, emptyMap<String, Any>()).queryResults()
    }
    fun parseNgsiLDandWriteToNeo4j(payload : String){
        //1.scan context and add namespaces if needed
        val context = this.jsonLDService.getContext(payload)

        /*for((k, v) in context.first()) {
            // insert namespaces
            println(v)
            //addNamespaceDefinition(k,v)
        }*/

        //
        val importStatement = "CALL apoc.load.json(payload) YIELD value AS payload  WITH payload, payload.id AS id, payload.type AS type\n" +
                "        MERGE (u:User {id:m.id}) ON CREATE SET u.initials = m.initials, u.name = m.fullname, u.user = m.username\n" +
                "        MERGE (b:Board {id: d.board.id}) ON CREATE SET b = d.board\n" +
                "        MERGE (c:Card {id: d.card.id}) ON CREATE SET c = d.card\n" +
                "        MERGE (u)-[r:CREATED]->(c) ON CREATE SET r.id = payload.id, r.date_created=apoc.date.parse(payload.date,'s',\"yyyy-MM-dd'T'HH:mm:ss'Z'\")\n" +
                "        MERGE (c)-[:IN_BOARD]->(b)\n" +
                "        RETURN count(*)"
        //queryResults = ogmSession.query(importStatement, emptyMap<String, Any>()).queryResults()
    }
}
