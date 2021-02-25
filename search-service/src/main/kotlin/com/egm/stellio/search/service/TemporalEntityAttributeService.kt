package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.isAttributeOfMeasureType
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonUtils
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

    fun createEntityTemporalReferences(payload: String, contexts: List<String>): Mono<Int> {
        val entity = JsonLdUtils.expandJsonLdEntity(payload, contexts).toNgsiLdEntity()
        val parsedPayload = JsonUtils.deserializeObject(payload)

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
                    value = valueToStringOrNull(it.second.value),
                    payload =
                        extractAttributeInstanceFromCompactedEntity(
                            parsedPayload,
                            compactTerm(it.first, contexts),
                            it.second.datasetId
                        )
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
            SELECT id, temporal_entity_attribute.entity_id, type, attribute_name, attribute_value_type, dataset_id
            FROM temporal_entity_attribute            
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

    fun getForEntityAndAttribute(id: URI, attributeName: String, datasetId: URI? = null): Mono<UUID> {
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
            datasetId = row.get("dataset_id", String::class.java)?.toUri()
        )
    }

    private var rowToId: ((Row) -> UUID) = { row ->
        row.get("id", UUID::class.java)!!
    }
}
