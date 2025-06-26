package com.egm.stellio.search.support

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.model.TemporalQuery.Aggregate
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_INSTANCE_ID_IRI
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.toNgsiLdAttributes
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
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
    lastN: Int? = null,
    instanceLimit: Int = 100,
    timeproperty: AttributeInstance.TemporalProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
) = TemporalQuery(
    timerel = timerel,
    timeAt = timeAt,
    endTimeAt = endTimeAt,
    aggrPeriodDuration = aggrPeriodDuration,
    aggrMethods = aggrMethods,
    lastN = lastN,
    instanceLimit = instanceLimit,
    timeproperty = timeproperty
)

fun buildDefaultPagination(
    limitDefault: Int = 50,
    limitMax: Int = 100,
    temporalLimit: Int = 100
) = ApplicationProperties.Pagination(
    limitDefault = limitDefault,
    limitMax = limitMax,
    temporalLimit = temporalLimit,
)

fun buildAttributeInstancePayload(
    value: Any,
    observedAt: ZonedDateTime,
    datasetId: URI? = null,
    instanceId: URI,
    attributeType: Attribute.AttributeType = Attribute.AttributeType.Property
): String = serializeObject(
    mutableMapOf(
        JSONLD_TYPE_KW to listOf(attributeType.toExpandedName()),
        NGSILD_OBSERVED_AT_IRI to buildNonReifiedTemporalValue(observedAt),
        NGSILD_INSTANCE_ID_IRI to buildNonReifiedPropertyValue(instanceId.toString())
    ).apply {
        if (datasetId != null)
            put(NGSILD_DATASET_ID_IRI, buildNonReifiedPropertyValue(datasetId.toString()))
        if (attributeType == Attribute.AttributeType.Property)
            put(NGSILD_PROPERTY_VALUE, listOf(mapOf(JSONLD_VALUE_KW to value)))
        else
            put(NGSILD_RELATIONSHIP_OBJECT, listOf(mapOf(JSONLD_ID_KW to value.toString())))
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

val SAMPLE_LOCATION_PROPERTY_PAYLOAD = Json.of(
    """
    {
      "https://uri.etsi.org/ngsi-ld/location": [
        {
          "@type": [
            "https://uri.etsi.org/ngsi-ld/GeoProperty"
          ],
          "https://uri.etsi.org/ngsi-ld/hasValue": [
            {
              "https://purl.org/geojson/vocab#coordinates": [
                {
                  "@list": [
                    {
                      "@value": 1
                    },
                    {
                      "@value": 1
                    }
                  ]
                }
              ],
              "@type": [
                "https://purl.org/geojson/vocab#Point"
              ]
            }
          ]
        }
      ]
    }
    """.trimIndent()
)
