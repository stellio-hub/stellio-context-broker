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

data class AttributeInstance private constructor(
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
            instanceId: URI = generateRandomInstanceId(),
            timeProperty: TemporalProperty,
            time: ZonedDateTime,
            modifiedAt: ZonedDateTime? = null,
            value: String? = null,
            measuredValue: Double? = null,
            geoValue: WKTCoordinates? = null,
            payload: ExpandedAttributeInstance,
            sub: String? = null
        ): AttributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute,
            instanceId = instanceId,
            timeProperty = timeProperty,
            time = time,
            value = value,
            measuredValue = measuredValue,
            geoValue = geoValue,
            payload = payload.composePayload(instanceId, modifiedAt).toJson(),
            sub = sub
        )

        private fun ExpandedAttributeInstance.toJson(): Json = Json.of(serializeObject(this))

        private fun ExpandedAttributeInstance.composePayload(
            instanceId: URI,
            modifiedAt: ZonedDateTime? = null
        ): ExpandedAttributeInstance =
            this.plus(NGSILD_INSTANCE_ID_PROPERTY to buildNonReifiedProperty(instanceId.toString()))
                .let {
                    if (modifiedAt != null)
                        it.plus(NGSILD_MODIFIED_AT_PROPERTY to buildNonReifiedDateTime(modifiedAt))
                    else it
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
