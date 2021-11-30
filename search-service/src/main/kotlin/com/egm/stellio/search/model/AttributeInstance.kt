package com.egm.stellio.search.model

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
    val observedAt: ZonedDateTime,
    val value: String? = null,
    val measuredValue: Double? = null,
    val payload: String
) {
    companion object {

        operator fun invoke(
            temporalEntityAttribute: UUID,
            instanceId: URI? = null,
            observedAt: ZonedDateTime,
            value: String? = null,
            measuredValue: Double? = null,
            payload: Map<String, Any>
        ): AttributeInstance {
            val parsedPayload = payload.toMutableMap()
            val attributeInstanceId = instanceId ?: generateRandomInstanceId()
            parsedPayload.putIfAbsent("instanceId", attributeInstanceId)

            return AttributeInstance(
                temporalEntityAttribute,
                attributeInstanceId,
                observedAt,
                value,
                measuredValue,
                serializeObject(parsedPayload)
            )
        }

        operator fun invoke(
            temporalEntityAttribute: UUID,
            instanceId: URI? = null,
            observedAt: ZonedDateTime,
            value: String? = null,
            measuredValue: Double? = null,
            jsonNode: JsonNode
        ): AttributeInstance {
            val attributeInstanceId = instanceId ?: generateRandomInstanceId()
            (jsonNode as ObjectNode).put("instanceId", attributeInstanceId.toString())

            return AttributeInstance(
                temporalEntityAttribute,
                attributeInstanceId,
                observedAt,
                value,
                measuredValue,
                serializeObject(jsonNode)
            )
        }

        private fun generateRandomInstanceId() = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri()
    }
}
