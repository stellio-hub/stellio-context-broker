package com.egm.stellio.search.temporal.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.common.util.toJson
import com.egm.stellio.search.entity.model.AttributeMetadata
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.NGSILD_INSTANCE_ID_IRI
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.UriUtils.toUri
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class AttributeInstance private constructor(
    val attributeUuid: UUID,
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
            attributeUuid: UUID,
            instanceId: URI = generateRandomInstanceId(),
            timeProperty: TemporalProperty? = TemporalProperty.OBSERVED_AT,
            modifiedAt: ZonedDateTime? = null,
            attributeMetadata: AttributeMetadata,
            payload: ExpandedAttributeInstance,
            time: ZonedDateTime,
            sub: String? = null
        ): AttributeInstance = AttributeInstance(
            attributeUuid = attributeUuid,
            instanceId = instanceId,
            timeProperty = timeProperty,
            time = time,
            value = attributeMetadata.value,
            measuredValue = attributeMetadata.measuredValue,
            geoValue = attributeMetadata.geoValue,
            payload = payload.addInstanceId(instanceId).addModifiedAt(modifiedAt).toJson(),
            sub = sub
        )

        operator fun invoke(
            attributeUuid: UUID,
            instanceId: URI = generateRandomInstanceId(),
            timeAndProperty: Pair<ZonedDateTime, TemporalProperty>,
            value: Triple<String?, Double?, WKTCoordinates?>,
            payload: ExpandedAttributeInstance,
            sub: String?
        ): AttributeInstance = AttributeInstance(
            attributeUuid = attributeUuid,
            instanceId = instanceId,
            timeProperty = timeAndProperty.second,
            time = timeAndProperty.first,
            value = value.first,
            measuredValue = value.second,
            geoValue = value.third,
            payload = payload.addInstanceId(instanceId).toJson(),
            sub = sub
        )

        private fun ExpandedAttributeInstance.addInstanceId(instanceId: URI): ExpandedAttributeInstance =
            this.plus(NGSILD_INSTANCE_ID_IRI to buildNonReifiedPropertyValue(instanceId.toString()))

        private fun ExpandedAttributeInstance.addModifiedAt(modifiedAt: ZonedDateTime?): ExpandedAttributeInstance =
            modifiedAt?.let {
                this.plus(NGSILD_MODIFIED_AT_IRI to buildNonReifiedTemporalValue(modifiedAt))
            } ?: this

        private fun generateRandomInstanceId() = "urn:ngsi-ld:Instance:${UUID.randomUUID()}".toUri()
    }

    // a TemporalProperty as defined in 4.8
    enum class TemporalProperty(val propertyName: String) {
        OBSERVED_AT("observedAt"),
        CREATED_AT("createdAt"),
        MODIFIED_AT("modifiedAt"),
        DELETED_AT("deletedAt");

        companion object {
            fun fromTimeProperty(timeProperty: String): Either<APIException, TemporalProperty> =
                entries.find { it.propertyName == timeProperty }.let {
                    it?.right() ?: BadRequestDataException("Unknown value for 'timeproperty': $timeProperty").left()
                }
        }
    }
}
