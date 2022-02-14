package com.egm.stellio.search.model

import arrow.core.Option
import arrow.core.toOption
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class AttributeInstance private constructor(
    val temporalEntityAttribute: UUID,
    val instanceId: URI,
    val timeProperty: TemporalProperty,
    val time: ZonedDateTime,
    val value: String? = null,
    val measuredValue: Double? = null,
    val payload: String
) {
    companion object {

        operator fun invoke(
            temporalEntityAttribute: UUID,
            instanceId: URI? = null,
            timeProperty: TemporalProperty,
            time: ZonedDateTime,
            value: String? = null,
            measuredValue: Double? = null,
            payload: Map<String, Any>
        ): AttributeInstance {
            val parsedPayload = payload.toMutableMap()
            val attributeInstanceId = instanceId ?: generateRandomInstanceId()
            parsedPayload.putIfAbsent("instanceId", attributeInstanceId)

            return AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute,
                instanceId = attributeInstanceId,
                timeProperty = timeProperty,
                time = time,
                value = value,
                measuredValue = measuredValue,
                payload = serializeObject(parsedPayload)
            )
        }

        operator fun invoke(
            temporalEntityAttribute: UUID,
            instanceId: URI? = null,
            timeProperty: TemporalProperty,
            time: ZonedDateTime,
            value: String? = null,
            measuredValue: Double? = null,
            jsonNode: JsonNode
        ): AttributeInstance {
            val attributeInstanceId = instanceId ?: generateRandomInstanceId()
            (jsonNode as ObjectNode).put("instanceId", attributeInstanceId.toString())

            return AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute,
                instanceId = attributeInstanceId,
                timeProperty = timeProperty,
                time = time,
                value = value,
                measuredValue = measuredValue,
                payload = serializeObject(jsonNode)
            )
        }

        private fun generateRandomInstanceId() = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri()
    }

    // a TemporalProperty as defined in 4.8
    enum class TemporalProperty(val propertyName: String) {
        OBSERVED_AT("observedAt"),
        CREATED_AT("createdAt"),
        MODIFIED_AT("modifiedAt");

        companion object {
            fun forPropertyName(propertyName: String): Option<TemporalProperty> =
                values().find { it.propertyName == propertyName }.toOption()
        }
    }
}
