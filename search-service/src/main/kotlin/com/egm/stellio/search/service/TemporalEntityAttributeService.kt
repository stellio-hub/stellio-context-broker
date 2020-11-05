package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.isAttributeOfMeasureType
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUES
import com.egm.stellio.shared.util.JsonLdUtils.expandValueAsListOfMap
import com.egm.stellio.shared.util.toUri
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
import java.time.format.DateTimeFormatter
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

    internal fun createEntityPayload(entityId: URI, entityPayload: String?): Mono<Int> =
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

    fun updateEntityPayload(entityId: URI, payload: String): Mono<Int> =
        databaseClient.execute("UPDATE entity_payload SET payload = :payload WHERE entity_id = :entity_id")
            .bind("payload", Json.of(payload))
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()

    fun createEntityTemporalReferences(payload: String): Mono<Int> {
        val entity = JsonLdUtils.expandJsonLdEntity(payload).toNgsiLdEntity()
        logger.debug("Analyzing create event for entity ${entity.id}")

        val temporalProperties = entity.properties
            .filter {
                // for now, let's say that if the 1st instance is temporal, all instances are temporal
                // let's also consider that a temporal property is one having an observedAt property
                it.instances[0].isTemporalAttribute()
            }.flatMapTo(
                arrayListOf(),
                {
                    it.instances.map { instance ->
                        Pair(it.name, instance)
                    }
                }
            )

        logger.debug("Found temporal properties for entity : $temporalProperties")
        if (temporalProperties.isEmpty())
            return Mono.just(0)

        return Flux.fromIterable(temporalProperties.asIterable())
            .map {
                val attributeValueType =
                    if (isAttributeOfMeasureType(it.second.value))
                        TemporalEntityAttribute.AttributeValueType.MEASURE
                    else
                        TemporalEntityAttribute.AttributeValueType.ANY
                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = entity.id,
                    type = entity.type,
                    attributeName = it.first,
                    attributeValueType = attributeValueType,
                    datasetId = it.second.datasetId,
                    entityPayload = payload
                )

                val attributeInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    observedAt = it.second.observedAt!!,
                    measuredValue = valueToDoubleOrNull(it.second.value),
                    value = valueToStringOrNull(it.second.value)
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

    fun getForEntity(id: URI, attrs: Set<String>): Flux<TemporalEntityAttribute> {
        val selectQuery =
            """
            SELECT id, temporal_entity_attribute.entity_id, type, attribute_name, attribute_value_type,
                payload::TEXT, dataset_id
            FROM temporal_entity_attribute
            LEFT JOIN entity_payload ON entity_payload.entity_id = temporal_entity_attribute.entity_id            
            WHERE temporal_entity_attribute.entity_id = :entity_id
            """.trimIndent()

        val expandedAttrsList = attrs.joinToString(",") { "'$it'" }

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

    fun getFirstForEntity(id: URI): Mono<UUID> {
        val selectQuery =
            """
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

    fun getForEntityAndAttribute(id: URI, attributeName: String, datasetId: String? = null): Mono<UUID> {
        val selectQuery =
            """
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
            entityId = row.get("entity_id", String::class.java)!!.toUri(),
            type = row.get("type", String::class.java)!!,
            attributeName = row.get("attribute_name", String::class.java)!!,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.valueOf(
                row.get(
                    "attribute_value_type",
                    String::class.java
                )!!
            ),
            datasetId = row.get("dataset_id", String::class.java)?.toUri(),
            entityPayload = row.get("payload", String::class.java)
        )
    }

    private var rowToId: ((Row) -> UUID) = { row ->
        row.get("id", UUID::class.java)!!
    }

    fun injectTemporalValues(
        jsonLdEntity: JsonLdEntity,
        rawResults: List<List<AttributeInstanceResult>>,
        withTemporalValues: Boolean
    ): JsonLdEntity {
        val resultEntity: MutableMap<String, List<Map<String, Any?>>> = mutableMapOf()
        val entity = jsonLdEntity.properties.toMutableMap()

        rawResults.filter {
            // filtering out empty lists
            it.isNotEmpty()
        }.forEach { attributeInstanceResults ->
            // attribute_name is the name of the temporal property we want to update
            val attributeName = attributeInstanceResults.first().attributeName
            // extract the temporal property from the raw entity
            // ... if it exists, which is not the case for notifications of a subscription
            // (in this case, create an empty map)
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
                val datasetId = rawDatasetId?.get(0)?.get(JSONLD_ID)
                datasetId == attributeInstanceResults.first().datasetId?.toString()
            }.map { instanceToEnrich ->
                if (withTemporalValues) {
                    instanceToEnrich.putIfAbsent(JSONLD_TYPE, NGSILD_PROPERTY_TYPE.uri)
                    // remove the existing value as we will inject our list of results in the property
                    instanceToEnrich.remove(NGSILD_PROPERTY_VALUE)

                    // Postgres stores the observedAt value in UTC.
                    // The value is retrieved as offsetDateTime and converted to the current timezone
                    // using the system variable timezone.
                    // For this reason, a cast to Instant with UTC as ZoneOffset is needed to create a ZonedDateTime.
                    val valuesMap =
                        attributeInstanceResults.map {
                            if (it.value is Double)
                                TemporalValue(
                                    it.value,
                                    it.observedAt.format(DateTimeFormatter.ISO_DATE_TIME)
                                )
                            else
                                RawValue(
                                    it.value,
                                    it.observedAt.format(DateTimeFormatter.ISO_DATE_TIME)
                                )
                        }
                    instanceToEnrich[NGSILD_PROPERTY_VALUES] = listOf(mapOf("@list" to valuesMap))
                    // and finally update the raw entity with the updated temporal property
                    resultEntity[attributeName] =
                        resultEntity[attributeName]?.plus(listOf(instanceToEnrich)) ?: listOf(instanceToEnrich)
                } else {
                    val valuesMap =
                        attributeInstanceResults.map {
                            val instance = mutableMapOf(
                                JSONLD_TYPE to NGSILD_PROPERTY_TYPE.uri,
                                NGSILD_INSTANCE_ID_PROPERTY to mapOf(
                                    JSONLD_ID to it.instanceId.toString()
                                ),
                                NGSILD_PROPERTY_VALUE to it.value,
                                NGSILD_OBSERVED_AT_PROPERTY to mapOf(
                                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                                    JSONLD_VALUE_KW to it.observedAt.format(DateTimeFormatter.ISO_DATE_TIME)
                                )
                            )
                            // a null datasetId should not be added to the valuesMap
                            if (it.datasetId != null)
                                instance[NGSILD_DATASET_ID_PROPERTY] =
                                    listOf(mapOf(JSONLD_ID to attributeInstanceResults.first().datasetId.toString()))

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

        return JsonLdEntity(entity, jsonLdEntity.contexts)
    }
}
