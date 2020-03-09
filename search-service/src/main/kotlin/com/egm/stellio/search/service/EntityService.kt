package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityTemporalProperty
import com.egm.stellio.search.model.TemporalValue
import com.egm.stellio.search.util.NgsiLdParsingUtils
import com.egm.stellio.search.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUES
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.isEquals
import org.springframework.data.r2dbc.query.Criteria.where
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.OffsetDateTime

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

    fun getForEntity(id: String, attrs: List<String>): Flux<EntityTemporalProperty> {
        var criteria = where("entity_id").isEquals(id)
        if (attrs.isNotEmpty())
            criteria = criteria.and("attribute_name").`in`(attrs)

        return databaseClient
            .select()
            .from(EntityTemporalProperty::class.java)
            .matching(criteria)
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

            val valuesMap = it.map { TemporalValue(it["VALUE"] as Double, (it["OBSERVED_AT"] as OffsetDateTime).toString()) }
            propertyToEnrich[NGSILD_PROPERTY_VALUES] = listOf(mapOf("@list" to valuesMap))

            // and finally update the raw entity with the updated temporal property
            entity.remove(expandedAttributeName)
            entity[expandedAttributeName!!] = listOf(propertyToEnrich)
        }

        return Pair(entity, rawEntity.second)
    }
}