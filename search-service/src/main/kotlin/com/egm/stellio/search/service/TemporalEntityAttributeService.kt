package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.RawValue
import com.egm.stellio.search.model.TemporalValue
import com.egm.stellio.search.util.isAttributeOfMeasureType
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUES
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandJsonLdKey
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandValueAsMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getPropertyValueFromMap
import com.egm.stellio.shared.util.NgsiLdParsingUtils.getPropertyValueFromMapAsDateTime
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.bind
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.*

@Service
class TemporalEntityAttributeService(
    private val databaseClient: DatabaseClient,
    private val attributeInstanceService: AttributeInstanceService
) {

    fun create(temporalEntityAttribute: TemporalEntityAttribute): Mono<Int> =
        databaseClient.execute("""
            INSERT INTO temporal_entity_attribute (id, entity_id, type, attribute_name, attribute_value_type, entity_payload)
            VALUES (:id, :entity_id, :type, :attribute_name, :attribute_value_type, :entity_payload)
            """)
            .bind("id", temporalEntityAttribute.id)
            .bind("entity_id", temporalEntityAttribute.entityId)
            .bind("type", temporalEntityAttribute.type)
            .bind("attribute_name", temporalEntityAttribute.attributeName)
            .bind("attribute_value_type", temporalEntityAttribute.attributeValueType.toString())
            .bind("entity_payload", temporalEntityAttribute.entityPayload?.let { Json.of(temporalEntityAttribute.entityPayload) })
            .fetch()
            .rowsUpdated()

    fun addEntityPayload(temporalEntityAttribute: TemporalEntityAttribute, payload: String): Mono<Int> =
        databaseClient.execute("UPDATE temporal_entity_attribute SET entity_payload = :entity_payload WHERE id = :id")
            .bind("entity_payload", Json.of(payload))
            .bind("id", temporalEntityAttribute.id)
            .fetch()
            .rowsUpdated()

    fun createEntityTemporalReferences(payload: String): Mono<Int> {

        val entity = NgsiLdParsingUtils.parseEntity(payload)
        val rawEntity = entity.attributes

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
                val expandedValues = expandValueAsMap(it.value)
                val attributeValue = getPropertyValueFromMap(expandedValues, NGSILD_PROPERTY_VALUE)!!
                val attributeValueType =
                    if (isAttributeOfMeasureType(attributeValue))
                        TemporalEntityAttribute.AttributeValueType.MEASURE
                    else
                        TemporalEntityAttribute.AttributeValueType.ANY
                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = rawEntity["@id"] as String,
                    type = (rawEntity["@type"] as List<*>)[0] as String,
                    attributeName = it.key,
                    attributeValueType = attributeValueType,
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
    }

    fun getForEntity(id: String, attrs: List<String>, contextLink: String): Flux<TemporalEntityAttribute> {
        val selectQuery = """
            SELECT id, entity_id, type, attribute_name, attribute_value_type, entity_payload::TEXT
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
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

    fun getForEntityAndAttribute(id: String, attritbuteName: String): Mono<UUID> {
        val selectQuery = """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            AND attribute_name = :attribute_name
        """.trimIndent()

        return databaseClient
            .execute(selectQuery)
            .bind("entity_id", id)
            .bind("attribute_name", attritbuteName)
            .map(rowToId)
            .one()
    }

    private var rowToTemporalEntityAttribute: ((Row) -> TemporalEntityAttribute) = { row ->
        TemporalEntityAttribute(
            id = row.get("id", UUID::class.java)!!,
            entityId = row.get("entity_id", String::class.java)!!,
            type = row.get("type", String::class.java)!!,
            attributeName = row.get("attribute_name", String::class.java)!!,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.valueOf(row.get("attribute_value_type", String::class.java)!!),
            entityPayload = row.get("entity_payload", String::class.java)
        )
    }

    private var rowToId: ((Row) -> UUID) = { row ->
        row.get("id", UUID::class.java)!!
    }

    fun injectTemporalValues(
        expandedEntity: ExpandedEntity,
        rawResults: List<List<Map<String, Any>>>,
        withTemporalValues: Boolean
    ): ExpandedEntity {

        val entity = expandedEntity.attributes.toMutableMap()

        rawResults.filter {
            // filtering out empty lists or lists with an empty map of results
            it.isNotEmpty() && it[0].isNotEmpty()
        }
        .forEach {
            // attribute_name is the name of the temporal property we want to update
            val attributeName = it.first()["attribute_name"]!! as String

            // extract the temporal property from the raw entity
            // ... if it exists, which is not the case for notifications of a subscription (in this case, create an empty map)
            val propertyToEnrich: MutableMap<String, Any> =
                if (entity[attributeName] != null) {
                    expandValueAsMap(entity[attributeName]!!).toMutableMap()
                } else {
                    mutableMapOf()
                }

            if (withTemporalValues) {
                propertyToEnrich.putIfAbsent(NGSILD_ENTITY_TYPE, NGSILD_PROPERTY_TYPE.uri)
                // remove the existing value as we will inject our list of results in the property
                propertyToEnrich.remove(NGSILD_PROPERTY_VALUE)

                // Postgres stores the observedAt value in UTC.
                // when retrieved, it's converted to the current timezone using the system variable timezone and with the +/- annotation.
                // For this reason, a cast to Instant is needed (with UTC as ZoneOffset) to return the expected dateTime with Z format.
                val valuesMap =
                    it.map {
                        if (it["value"] is Double)
                            TemporalValue(it["value"] as Double, (it["observed_at"] as ZonedDateTime).toInstant().atZone(ZoneOffset.UTC).toString())
                        else
                            RawValue(it["value"]!!, (it["observed_at"] as ZonedDateTime).toInstant().atZone(ZoneOffset.UTC).toString())
                    }
                propertyToEnrich[NGSILD_PROPERTY_VALUES] = listOf(mapOf("@list" to valuesMap))

                // and finally update the raw entity with the updated temporal property
                entity[attributeName] = listOf(propertyToEnrich)
            } else {
                val valuesMap =
                    it.map {
                        mapOf(NGSILD_ENTITY_TYPE to NGSILD_PROPERTY_TYPE.uri,
                            NGSILD_INSTANCE_ID_PROPERTY to mapOf(
                                NGSILD_ENTITY_ID to it["instance_id"]
                            ),
                            NGSILD_PROPERTY_VALUE to it["value"],
                            NGSILD_OBSERVED_AT_PROPERTY to mapOf(
                                NGSILD_ENTITY_TYPE to NGSILD_DATE_TIME_TYPE,
                                JSONLD_VALUE_KW to (it["observed_at"] as ZonedDateTime).toInstant().atZone(ZoneOffset.UTC).toString()
                            )
                        )
                    }

                entity[attributeName] = listOf(valuesMap)
            }
        }

        return ExpandedEntity(entity, expandedEntity.contexts)
    }
}
