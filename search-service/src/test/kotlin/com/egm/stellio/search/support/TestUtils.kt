package com.egm.stellio.search.support

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.toNgsiLdAttribute
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.shouldSucceedAndResult
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime

fun buildAttributeInstancePayload(
    value: Any,
    observedAt: ZonedDateTime,
    datasetId: URI? = null,
    instanceId: URI? = null,
    attributeType: TemporalEntityAttribute.AttributeType = TemporalEntityAttribute.AttributeType.Property
) = serializeObject(
    mapOf(
        "type" to attributeType.toString(),
        "datasetId" to datasetId,
        "instanceId" to instanceId,
        "observedAt" to observedAt
    )
        .let {
            if (attributeType == TemporalEntityAttribute.AttributeType.Property)
                it.plus("value" to value)
            else
                it.plus("object" to value)
        }
)

suspend fun buildSapAttribute(specificAccessPolicy: AuthContextModel.SpecificAccessPolicy): NgsiLdAttribute {
    val sapPropertyFragment =
        """
        {
            "type": "Property",
            "value": "$specificAccessPolicy"
        }
        """.trimIndent()

    return expandAttribute(AUTH_TERM_SAP, sapPropertyFragment, AuthContextModel.AUTHORIZATION_API_DEFAULT_CONTEXTS)
        .toNgsiLdAttribute().shouldSucceedAndResult()
}

const val EMPTY_PAYLOAD = "{}"
val EMPTY_JSON_PAYLOAD = Json.of(EMPTY_PAYLOAD)
