package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils
import java.net.URI
import java.time.ZonedDateTime

fun buildAttributeInstancePayload(
    value: Any,
    observedAt: ZonedDateTime,
    datasetId: URI? = null,
    instanceId: URI? = null,
    attributeType: TemporalEntityAttribute.AttributeType = TemporalEntityAttribute.AttributeType.Property
) = JsonUtils.serializeObject(
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

fun buildSapAttribute(specificAccessPolicy: AuthContextModel.SpecificAccessPolicy): NgsiLdAttribute {
    val sapPropertyFragment =
        """
        {
            "type": "Property",
            "value": "$specificAccessPolicy"
        }
        """.trimIndent()

    return parseToNgsiLdAttributes(
        JsonLdUtils.expandJsonLdFragment(AUTH_TERM_SAP, sapPropertyFragment, AuthContextModel.COMPOUND_AUTHZ_CONTEXT)
    )[0]
}
