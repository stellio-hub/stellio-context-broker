package com.egm.datahub.context.search.service

import com.egm.datahub.context.search.model.EntityTemporalProperty
import com.egm.datahub.context.search.util.NgsiLdParsingUtils
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.isEquals
import org.springframework.data.r2dbc.query.Criteria.where
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class EntityService(
    private val databaseClient: DatabaseClient
) {

    fun createEntityTemporalReferences(entity: Pair<Map<String, Any>, List<String>>): Mono<Int> {

        val rawEntity = entity.first

        val temporalProperties = rawEntity
            .filter {
                it.value is List<*>
            }
            .filter {
                // TODO abstract this crap into an NgsiLdParsingUtils function
                val entryValue = (it.value as List<*>)[0]
                if (entryValue is Map<*, *>) {
                    val values = (it.value as List<*>)[0] as Map<String, Any>
                    values.containsKey("https://uri.etsi.org/ngsi-ld/observedAt")
                } else {
                    false
                }
            }

        return Flux.fromIterable(temporalProperties.asIterable())
            .map {
                // TODO abstract this crap into an NgsiLdParsingUtils function
                val propertyValues = (it.value as List<*>)[0] as Map<String, Any>
                val observedByProperty = (propertyValues["https://ontology.eglobalmark.com/egm#observedBy"] as List<*>)[0] as Map<String, Any>
                val observedBy = ((observedByProperty["https://uri.etsi.org/ngsi-ld/hasObject"] as List<*>)[0] as Map<*, *>)["@id"] as String
                EntityTemporalProperty(
                    entityId = rawEntity["@id"] as String,
                    type = (rawEntity["@type"] as List<*>)[0] as String,
                    attributeName = it.key,
                    observedBy = observedBy
                )
            }
            .flatMap {
                databaseClient.insert()
                    .into(EntityTemporalProperty::class.java)
                    .using(it)
                    .fetch()
                    .rowsUpdated()
            }
            .collectList()
            .map { it.size }
    }

    fun getForEntity(id: String): Flux<EntityTemporalProperty> {
        return databaseClient
            .select()
            .from(EntityTemporalProperty::class.java)
            .matching(where("entity_id").isEquals(id))
            .fetch()
            .all()
    }

    fun injectTemporalValues(rawEntity: Pair<Map<String, Any>, List<String>>, rawResults: List<List<Map<String, Any>>>): Pair<Map<String, Any>, List<String>> {

        val entity = rawEntity.first.toMutableMap()

        rawResults.forEach {
            // attribute_name is the name of the temporal property we want to update
            val attributeName = it.first()["attribute_name"]!!
            val expandedAttributeName = NgsiLdParsingUtils.expandJsonLdKey(attributeName as String, rawEntity.second)

            // extract the temporal property from the raw entity and remove the value property from it
            val propertyToEnrich = NgsiLdParsingUtils.expandValueAsMap(entity[expandedAttributeName]!!).toMutableMap()
            propertyToEnrich.remove(NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE)

            // insert the values property with data retrieved from DB
            val valuesKey = NgsiLdParsingUtils.expandJsonLdKey("values", rawEntity.second)
            val simplifiedValues = it.map { "[${it["VALUE"]}, \"${it["OBSERVED_AT"]}\"]" }.joinToString(",")
            propertyToEnrich[valuesKey!!] = listOf(simplifiedValues)

            // and finally update the raw entity with the updated temporal property
            entity.remove(expandedAttributeName)
            entity[expandedAttributeName!!] = listOf(propertyToEnrich)
        }

        return Pair(entity, rawEntity.second)
    }
}