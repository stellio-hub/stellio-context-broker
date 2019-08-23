package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.service.JsonLDService
import com.egm.datahub.context.registry.web.AlreadyExistingEntityException
import org.neo4j.ogm.session.Session
import org.springframework.http.HttpEntity
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate
import org.springframework.web.reactive.function.server.RequestPredicates.headers
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpMethod
import org.springframework.http.ResponseEntity
import org.springframework.http.client.support.BasicAuthorizationInterceptor









@Component
class Neo4jRepository(
        private val ogmSession: Session,
        private val jsonLDService: JsonLDService
) {

    fun createEntity(jsonld: String) : Long {
        val entityUrn = jsonLDService.parsePayload(jsonld)
        if(getByURI(entityUrn).size > 0){
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
        return queryResults.first()["triplesLoaded"] as Long
    }

    fun getByURI(uri: String) : List<Map<String, Any>> {
        val pattern = "{ uri: '$uri' }"
        val matchQuery = """
            MATCH (n $pattern ) RETURN n
        """.trimIndent()
        val result = ogmSession.query(matchQuery, emptyMap<String, Any>())
        return result.queryResults().toList()
    }


    fun getEntitiesByLabel(label: String) : List<Map<String, Any>> {
        return performQuery("{ \"cypher\" : \"MATCH (o:$label) OPTIONAL MATCH (o:$label)-[r]-(s )  RETURN o , r \" , \"format\": \"JSON-LD\" }")
    }

    fun getEntitiesByLabelAndQuery(query: String, label: String) : List<Map<String, Any>> {
        var q = ""
        //RELATION
        if(query.split("==")[1].startsWith("urn:")){
            val rel = query.split("==")[0]
            val node = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"MATCH (o:$label)-[r:$rel]-(s { uri : \\\"$node\\\" })  RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        } else{
            //PROPERTY
            val property = query.split("==")[0]
            val value = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"MATCH (o:$label { $property : \\\"$value\\\" }) OPTIONAL MATCH (o:$label { $property : \\\"$value\\\" })-[r]-(s) RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        }
    }

    fun getEntitiesByQuery(query: String) : List<Map<String, Any>> {
        var q = ""
        if(query.split("==")[1].startsWith("urn:")){
            val rel = query.split("==")[0]
            val node = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"OPTIONAL MATCH (o)-[r:$rel]-(s { uri : \\\"$node\\\" })  RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        } else{
            val property = query.split("==")[0]
            val value = query.split("==")[1]
            val cypher = "{ \"cypher\" : \"MATCH (o { $property : \\\"$value\\\" }) OPTIONAL MATCH (o { $property : \\\"$value\\\" })-[r]-(s)  RETURN o,r\" , \"format\": \"JSON-LD\" }"
            return performQuery(cypher)
        }
    }

    fun performQuery(cypher : String) : List<Map<String, Any>>{
        val headers = LinkedMultiValueMap<String, String>()
        headers.add("Content-Type", "application/json")
        headers.add("Accept", "application/ld+json")
        val request = HttpEntity<String>(cypher, headers)
        try {
            val restTemplate= RestTemplate()
            restTemplate.getInterceptors().add(
                    BasicAuthorizationInterceptor("neo4j", "test"))
            val response = restTemplate.exchange(
                    "http://docker:7474/rdf/cypheronrdf",
                    HttpMethod.POST,
                    request,
                    object : ParameterizedTypeReference<List<Map<String, Any>>>() {

                    }).body.orEmpty()
            return response
        } catch(e : Exception){
            println(e.stackTrace)
        }
        return emptyList()
    }

}
