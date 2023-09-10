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
    val timeProperty: TemporalProperty? = TemporalProperty.OBSERVED_AT,
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
            timeProperty: TemporalProperty? = TemporalProperty.OBSERVED_AT,
            modifiedAt: ZonedDateTime? = null,
            attributeMetadata: AttributeMetadata,
            payload: ExpandedAttributeInstance,
            time: ZonedDateTime,
            sub: String? = null
        ): AttributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute,
            instanceId = instanceId,
            timeProperty = timeProperty,
            time = time,
            value = attributeMetadata.value,
            measuredValue = attributeMetadata.measuredValue,
            geoValue = attributeMetadata.geoValue,
            payload = payload.composePayload(instanceId, modifiedAt).toJson(),
            sub = sub
        )

        operator fun invoke(
            temporalEntityAttribute: UUID,
            instanceId: URI = generateRandomInstanceId(),
            timeAndProperty: Pair<ZonedDateTime, TemporalProperty>,
            value: Triple<String?, Double?, WKTCoordinates?>,
            payload: ExpandedAttributeInstance,
            sub: String?
        ): AttributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute,
            instanceId = instanceId,
            timeProperty = timeAndProperty.second,
            time = timeAndProperty.first,
            value = value.first,
            measuredValue = value.second,
            geoValue = value.third,
            payload = payload.composePayload(instanceId).toJson(),
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
                entries.find { it.propertyName == propertyName }
        }
    }
}
