package com.egm.stellio.search.model

import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.ExpandedAttributePayloadEntry
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedProperty
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
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
    val geoValue: WKTCoordinates? = null,
    val payload: String,
    val sub: String? = null
) {
    companion object {

        operator fun invoke(
            temporalEntityAttribute: UUID,
            instanceId: URI? = null,
            timeProperty: TemporalProperty,
            time: ZonedDateTime,
            value: String? = null,
            measuredValue: Double? = null,
            geoValue: WKTCoordinates? = null,
            payload: ExpandedAttributePayloadEntry,
            sub: String? = null
        ): AttributeInstance {
            val parsedPayload = payload.toMutableMap()
            val attributeInstanceId = instanceId ?: generateRandomInstanceId()
            parsedPayload.putIfAbsent(
                NGSILD_INSTANCE_ID_PROPERTY,
                buildNonReifiedProperty(attributeInstanceId.toString())
            )

            return AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute,
                instanceId = attributeInstanceId,
                timeProperty = timeProperty,
                time = time,
                value = value,
                measuredValue = measuredValue,
                geoValue = geoValue,
                payload = serializeObject(parsedPayload),
                sub = sub
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
            fun forPropertyName(propertyName: String): TemporalProperty? =
                values().find { it.propertyName == propertyName }
        }
    }
}
