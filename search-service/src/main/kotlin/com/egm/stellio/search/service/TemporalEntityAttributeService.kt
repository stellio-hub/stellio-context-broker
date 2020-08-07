package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.isAttributeOfMeasureType
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUES
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandJsonLdKey
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandValueAsListOfMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getPropertyValueFromMapAsUri
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

@Service
class TemporalEntityAttributeService(
    private val databaseClient: DatabaseClient,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun create(temporalEntityAttribute: TemporalEntityAttribute): Mono<Int> =
        databaseClient.execute(
            """
            INSERT INTO temporal_entity_attribute (id, entity_id, type, attribute_name, attribute_value_type, dataset_id)
            VALUES (:id, :entity_id, :type, :attribute_name, :attribute_value_type, :dataset_id)
            """
        )
            .bind("id", temporalEntityAttribute.id)
            .bind("entity_id", temporalEntityAttribute.entityId)
            .bind("type", temporalEntityAttribute.type)
            .bind("attribute_name", temporalEntityAttribute.attributeName)
            .bind("attribute_value_type", temporalEntityAttribute.attributeValueType.toString())
            .bind("dataset_id", temporalEntityAttribute.datasetId)
            .fetch()
            .rowsUpdated()

    internal fun createEntityPayload(entityId: String, entityPayload: String?): Mono<Int> =
        databaseClient.execute(
            """
            INSERT INTO entity_payload (entity_id, payload)
            VALUES (:entity_id, :payload)
            """
        )
            .bind("entity_id", entityId)
            .bind("payload", entityPayload?.let { Json.of(entityPayload) })
            .fetch()
            .rowsUpdated()

    fun updateEntityPayload(entityId: String, payload: String): Mono<Int> =
        databaseClient.execute("UPDATE entity_payload SET payload = :payload WHERE entity_id = :entity_id")
            .bind("payload", Json.of(payload))
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()

    fun createEntityTemporalReferences(payload: String): Mono<Int> {

        val entity = NgsiLdParsingUtils.parseEntity(payload)
        logger.debug("Analyzing create event for entity ${entity.id}")

        val temporalProperties = entity.properties
            .filter {
                // for now, let's say that if the 1st instance is temporal, all instances are temporal
                // let's also consider that a temporal property is one having an observedAt property
                it.value[0].containsKey(NGSILD_OBSERVED_AT_PROPERTY)
            }.flatMapTo(arrayListOf(), {
                it.value.map { instance ->
                    Pair(it.key, instance)
                }
            })

        logger.debug("Found temporal properties for entity : $temporalProperties")
        if (temporalProperties.isEmpty())
            return Mono.just(0)

        return Flux.fromIterable(temporalProperties.asIterable())
            .map {
                val expandedValues = it.second
                val attributeValue = getPropertyValueFromMap(expandedValues, NGSILD_PROPERTY_VALUE)!!
                val attributeValueType =
                    if (isAttributeOfMeasureType(attributeValue))
                        TemporalEntityAttribute.AttributeValueType.MEASURE
                    else
                        TemporalEntityAttribute.AttributeValueType.ANY
                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = entity.id,
                    type = entity.type,
                    attributeName = it.first,
                    attributeValueType = attributeValueType,
                    datasetId = getPropertyValueFromMapAsUri(expandedValues, NGSILD_DATASET_ID_PROPERTY),
                    entityPayload = payload
                )

                val observedAt = getPropertyValueFromMapAsDateTime(expandedValues, NGSILD_OBSERVED_AT_PROPERTY)!!
                val attributeInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    observedAt = observedAt,
                    measuredValue = valueToDoubleOrNull(attributeValue),
                    value = valueToStringOrNull(attributeValue)
                )

                Pair(temporalEntityAttribute, attributeInstance)
            }
            .flatMap { temporalEntityAttributeAndInstance ->
                create(temporalEntityAttributeAndInstance.first).zipWhen {
                    attributeInstanceService.create(temporalEntityAttributeAndInstance.second)
                }
            }
            .collectList()
            .map { it.size }
            .zipWith(createEntityPayload(entity.id, payload))
            .map { it.t1 + it.t2 }
    }

    fun getForEntity(id: String, attrs: List<String>, contextLink: String): Flux<TemporalEntityAttribute> {
        val selectQuery = """
            SELECT id, temporal_entity_attribute.entity_id, type, attribute_name, attribute_value_type, entity_payload::TEXT, dataset_id
            FROM temporal_entity_attribute
            LEFT JOIN entity_payload ON entity_payload.entity_id = temporal_entity_attribute.entity_id            
            WHERE temporal_entity_attribute.entity_id = :entity_id
            """.trimIndent()

        val expandedAttrsList =
            attrs.map {
                expandJsonLdKey(it, contextLink)!!
            }
                .joinToString(",") { "'$it'" }

        val finalQuery =
            if (attrs.isNotEmpty())
                "$selectQuery AND attribute_name in ($expandedAttrsList)"
            else
                selectQuery

        return databaseClient
            .execute(finalQuery)
            .bind("entity_id", id)
            .map(rowToTemporalEntityAttribute)
            .all()
    }

    fun getFirstForEntity(id: String): Mono<UUID> {
        val selectQuery = """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            """.trimIndent()

        return databaseClient
            .execute(selectQuery)
            .bind("entity_id", id)
            .map(rowToId)
            .first()
    }

    fun getForEntityAndAttribute(id: String, attributeName: String, datasetId: String? = null): Mono<UUID> {
        val selectQuery = """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            ${if (datasetId != null) "AND dataset_id = :dataset_id" else ""}
            AND attribute_name = :attribute_name
            """.trimIndent()

        return databaseClient
            .execute(selectQuery)
            .bind("entity_id", id)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .map(rowToId)
            .one()
    }

    private var rowToTemporalEntityAttribute: ((Row) -> TemporalEntityAttribute) = { row ->
        TemporalEntityAttribute(
            id = row.get("id", UUID::class.java)!!,
            entityId = row.get("entity_id", String::class.java)!!,
            type = row.get("type", String::class.java)!!,
            attributeName = row.get("attribute_name", String::class.java)!!,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.valueOf(
                row.get(
                    "attribute_value_type",
                    String::class.java
                )!!
            ),
            datasetId = row.get("dataset_id", String::class.java)?.let { URI.create(it) },
            entityPayload = row.get("entity_payload", String::class.java)
        )
    }

    private var rowToId: ((Row) -> UUID) = { row ->
        row.get("id", UUID::class.java)!!
    }

    private var rowToCount: ((Row) -> Int) = { row ->
        row.get("count", Integer::class.java)!!.toInt()
    }

    fun injectTemporalValues(
        expandedEntity: ExpandedEntity,
        rawResults: List<List<AttributeInstanceResult>>,
        withTemporalValues: Boolean
    ): ExpandedEntity {

        val resultEntity: MutableMap<String, List<Map<String, Any?>>> = mutableMapOf()
        val entity = expandedEntity.rawJsonLdProperties.toMutableMap()
        rawResults.filter {
            // filtering out empty lists
            it.isNotEmpty()
        }.forEach { attributeInstanceResults ->
            // attribute_name is the name of the temporal property we want to update
            val attributeName = attributeInstanceResults.first().attributeName
            // extract the temporal property from the raw entity
            // ... if it exists, which is not the case for notifications of a subscription (in this case, create an empty map)
            val propertyToEnrich: List<MutableMap<String, Any>> =
                if (entity[attributeName] != null)
                    expandValueAsListOfMap(entity[attributeName]!!).map {
                        it.toMutableMap() as MutableMap<String, Any>
                    }
                else
                    listOf(mutableMapOf())

            // get the instance that matches the datasetId of the rawResult
            propertyToEnrich.filter { instanceToEnrich ->
                val rawDatasetId = instanceToEnrich[NGSILD_DATASET_ID_PROPERTY] as List<Map<String, String?>>?
                val datasetId = rawDatasetId?.get(0)?.get(NGSILD_ENTITY_ID)
                datasetId == attributeInstanceResults.first().datasetId?.toString()
            }
            .map { instanceToEnrich ->
                if (withTemporalValues) {
                    instanceToEnrich.putIfAbsent(NGSILD_ENTITY_TYPE, NGSILD_PROPERTY_TYPE.uri)
                    // remove the existing value as we will inject our list of results in the property
                    instanceToEnrich.remove(NGSILD_PROPERTY_VALUE)

                    // Postgres stores the observedAt value in UTC.
                    // The value is retrieved as offsetDateTime and converted to the current timezone using the system variable timezone.
                    // For this reason, a cast to Instant with UTC as ZoneOffset is needed to create a ZonedDateTime.
                    val valuesMap =
                        attributeInstanceResults.map {
                            if (it.value is Double)
                                TemporalValue(
                                    it.value as Double,
                                    it.observedAt.toString()
                                )
                            else
                                RawValue(
                                    it.value,
                                    it.observedAt.toString()
                                )
                        }
                    instanceToEnrich[NGSILD_PROPERTY_VALUES] = listOf(mapOf("@list" to valuesMap))
                    // and finally update the raw entity with the updated temporal property
                    resultEntity[attributeName] = resultEntity[attributeName]?.plus(listOf(instanceToEnrich)) ?: listOf(instanceToEnrich)
                } else {
                    val valuesMap =
                        attributeInstanceResults.map {
                            val instance = mutableMapOf(
                                NGSILD_ENTITY_TYPE to NGSILD_PROPERTY_TYPE.uri,
                                NGSILD_INSTANCE_ID_PROPERTY to mapOf(
                                    NGSILD_ENTITY_ID to it.instanceId.toString()
                                ),
                                NGSILD_PROPERTY_VALUE to it.value,
                                NGSILD_OBSERVED_AT_PROPERTY to mapOf(
                                    NGSILD_ENTITY_TYPE to NGSILD_DATE_TIME_TYPE,
                                    JSONLD_VALUE_KW to it.observedAt.toString()
                                )
                            )
                            // a null datasetId should not be added to the valuesMap
                            if (it.datasetId != null)
                                instance[NGSILD_DATASET_ID_PROPERTY] = listOf(mapOf(NGSILD_ENTITY_ID to attributeInstanceResults.first().datasetId.toString()))

                            instance
                        }

                    resultEntity[attributeName] = (resultEntity[attributeName]?.plus(valuesMap) ?: (valuesMap))
                }
            }
        }

        // inject temporal values in the entity to be returned (replace entity properties by their temporal evolution)
        resultEntity.forEach {
            entity[it.key] = it.value
        }

        return ExpandedEntity(entity, expandedEntity.contexts)
    }
}
