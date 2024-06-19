package com.egm.stellio.search.support

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.model.TemporalQuery.Aggregate
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.toNgsiLdAttributes
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedTemporalValue
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.shouldSucceedAndResult
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.ZonedDateTime

@SuppressWarnings("LongParameterList")
fun buildDefaultTestTemporalQuery(
    timerel: TemporalQuery.Timerel? = null,
    timeAt: ZonedDateTime? = null,
    endTimeAt: ZonedDateTime? = null,
    aggrPeriodDuration: String? = null,
    aggrMethods: List<Aggregate>? = null,
    asLastN: Boolean = false,
    limit: Int = 100,
    timeproperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
) = TemporalQuery(
    timerel = timerel,
    timeAt = timeAt,
    endTimeAt = endTimeAt,
    aggrPeriodDuration = aggrPeriodDuration,
    aggrMethods = aggrMethods,
    asLastN = asLastN,
    limit = limit,
    timeproperty = timeproperty
)

fun buildDefaultPagination(
    limitDefault: Int = 50,
    limitMax: Int = 100,
    temporalLimitDefault: Int = 100,
    temporalLimitMax: Int = 1000
) = ApplicationProperties.Pagination(
    limitDefault = limitDefault,
    limitMax = limitMax,
    temporalLimitDefault = temporalLimitDefault,
    temporalLimitMax = temporalLimitMax,
)

fun buildAttributeInstancePayload(
    value: Any,
    observedAt: ZonedDateTime,
    datasetId: URI? = null,
    instanceId: URI,
    attributeType: TemporalEntityAttribute.AttributeType = TemporalEntityAttribute.AttributeType.Property
): String = serializeObject(
    mutableMapOf(
        JSONLD_TYPE to listOf(attributeType.toExpandedName()),
        NGSILD_OBSERVED_AT_PROPERTY to buildNonReifiedTemporalValue(observedAt),
        NGSILD_INSTANCE_ID_PROPERTY to buildNonReifiedPropertyValue(instanceId.toString())
    ).apply {
        if (datasetId != null)
            put(NGSILD_DATASET_ID_PROPERTY, buildNonReifiedPropertyValue(datasetId.toString()))
        if (attributeType == TemporalEntityAttribute.AttributeType.Property)
            put(NGSILD_PROPERTY_VALUE, listOf(mapOf(JSONLD_VALUE to value)))
        else
            put(NGSILD_RELATIONSHIP_OBJECT, listOf(mapOf(JSONLD_ID to value.toString())))
    }
)

suspend fun buildSapAttribute(specificAccessPolicy: AuthContextModel.SpecificAccessPolicy): NgsiLdAttribute =
    mapOf(AUTH_PROP_SAP to buildExpandedPropertyValue(specificAccessPolicy))
        .toNgsiLdAttributes()
        .shouldSucceedAndResult()[0]

const val EMPTY_PAYLOAD = "{}"
val EMPTY_JSON_PAYLOAD = Json.of(EMPTY_PAYLOAD)
val SAMPLE_JSON_PROPERTY_PAYLOAD = Json.of(
    """
    {
      "id": "123",
      "stringValue": "value",
      "nullValue": null
    }
    """.trimIndent()
)
val SAMPLE_LANGUAGE_PROPERTY_PAYLOAD = Json.of(
    """
    {
      "https://uri.etsi.org/ngsi-ld/hasLanguageMap": [
        {
          "@value": "My beautiful beehive",
          "@language": "en"
        },
        {
          "@value": "Ma belle ruche",
          "@language": "fr"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/LanguageProperty"
      ]
    }
    """.trimIndent()
)
val SAMPLE_VOCAB_PROPERTY_PAYLOAD = Json.of(
    """
    {
      "https://uri.etsi.org/ngsi-ld/hasVocab": [
        {
          "@id": "https://uri.etsi.org/ngsi-ld/default-context/stellio"
        },
        {
          "@id": "https://uri.etsi.org/ngsi-ld/default-context/egm"
        }
      ],
      "@type": [
        "https://uri.etsi.org/ngsi-ld/VocabProperty"
      ]
    }
    """.trimIndent()
)
