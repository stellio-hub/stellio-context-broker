package com.egm.stellio.search.model

import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.ExpandedAttributeInstance
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedDateTime
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedProperty
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class AttributeInstance constructor(
    val temporalEntityAttribute: UUID,
    val instanceId: URI,
    val timeProperty: TemporalProperty,
    val time: ZonedDateTime,
    val value: String? = null,
    val measuredValue: Double? = null,
    val geoValue: WKTCoordinates? = null,
    val payload: Json,
    val sub: String? = null
) {
    companion object {

        operator fun invoke(
            temporalEntityAttribute: UUID,
            instanceId: URI? = null,
            timeProperty: TemporalProperty,
            time: ZonedDateTime,
            modifiedAt: ZonedDateTime? = null,
            value: String? = null,
            measuredValue: Double? = null,
            geoValue: WKTCoordinates? = null,
            payload: ExpandedAttributeInstance,
            sub: String? = null
        ): AttributeInstance {
            val parsedPayload = payload.toMutableMap()
            val attributeInstanceId = instanceId ?: generateRandomInstanceId()
            parsedPayload.putIfAbsent(
                NGSILD_INSTANCE_ID_PROPERTY,
                buildNonReifiedProperty(attributeInstanceId.toString())
            )
            modifiedAt?.let {
                parsedPayload.putIfAbsent(
                    NGSILD_MODIFIED_AT_PROPERTY,
                    buildNonReifiedDateTime(it)
                )
            }

            return AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute,
                instanceId = attributeInstanceId,
                timeProperty = timeProperty,
                time = time,
                value = value,
                measuredValue = measuredValue,
                geoValue = geoValue,
                payload = Json.of(serializeObject(parsedPayload)),
                sub = sub
            )
        }

        fun generateRandomInstanceId() = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri()
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
