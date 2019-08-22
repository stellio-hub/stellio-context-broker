package com.egm.datahub.context.registry.repository

import com.egm.datahub.context.registry.service.JsonLDService
import com.egm.datahub.context.registry.web.AlreadyExistingEntityException
import org.neo4j.ogm.session.Session
import org.springframework.stereotype.Component

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


    fun getRelatedEntitiesByLabel(label: String) : List<Map<String, Any>> {
        val matchQuery = """
            MATCH (a)-[b]-(c:$label)  RETURN *
        """.trimIndent()
        val result = ogmSession.query(matchQuery, emptyMap<String, Any>())
        return result.queryResults().toList()
    }

    fun getEntitiesByLabelandQuery(query: String, label: String) : List<Map<String, Any>> {
        var q = ""
        //RELATION
        if(query.split("==")[1].startsWith("urn:")){
            val rel = query.split("==")[0]
            val node = query.split("==")[1]
            q = "MATCH (s)-[r:$rel]-(o:$label {uri: '$node'}) RETURN s,o"
        } else{
            //PROPERTY
            val property = query.split("==")[0]
            val value = query.split("==")[1]
            q = "MATCH ()-[]-(n:$label{$property:'$value'} ) RETURN n"
        }
        val matchQuery = """
            $q
        """.trimIndent()
        val result = ogmSession.query(matchQuery, emptyMap<String, Any>())
        return result.queryResults().toList()
    }

    fun getEntitiesByLabel(label: String) : List<Map<String, Any>> {
        val matchQuery = """
            MATCH (n:$label) RETURN n
        """.trimIndent()
        val result = ogmSession.query(matchQuery, emptyMap<String, Any>())
        return result.queryResults().toList()
    }

    fun getEntitiesByQuery(query: String) : List<Map<String, Any>> {
        var query = ""
        if(query.split("==")[1].startsWith("urn:")){
            val rel = query.split("==")[0]
            val node = query.split("==")[1]
            query = "MATCH ()-[r:$rel]-(n{uri:'$node'}) RETURN n"
        } else{
            val property = query.split("==")[0]
            val value = query.split("==")[1]
            query = "MATCH ()-[]-(n{$property:$value} ) RETURN n"
        }
        val matchQuery = """
            $query
        """.trimIndent()
        val result = ogmSession.query(matchQuery, emptyMap<String, Any>())
        return result.queryResults().toList()
    }
    fun getEntities() : List<Map<String, Any>> {
        val matchQuery = """
            MATCH (n:Resource) RETURN n
        """.trimIndent()
        val result = ogmSession.query(matchQuery, emptyMap<String, Any>())
        return result.queryResults().toList()
    }
}
