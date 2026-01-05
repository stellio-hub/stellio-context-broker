package com.egm.stellio.subscription.model

import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.subscription.model.Endpoint.AcceptType.JSON
import com.fasterxml.jackson.annotation.JsonProperty
import java.net.URI

data class Endpoint(
    val uri: URI,
    val accept: AcceptType = JSON,
    val cooldown: Int? = null,
    val notifierInfo: List<EndpointInfo>? = null,
    val receiverInfo: List<EndpointInfo>? = null,
    val timeout: Int = DEFAULT_TIMEOUT
) {

    enum class AcceptType(val accept: String) {
        @JsonProperty("application/json")
        JSON("application/json"),

        @JsonProperty("application/ld+json")
        JSONLD("application/ld+json"),

        @JsonProperty("application/geo+json")
        GEOJSON("application/geo+json")
    }

    companion object {

        // set a default timeout of 30 seconds when none specified
        const val DEFAULT_TIMEOUT = 30000

        const val MQTT_SCHEME = "mqtt"
        const val MQTTS_SCHEME = "mqtts"
        const val HTTP_SCHEME = "http"
        const val HTTPS_SCHEME = "https"
        val allowedSchemes = listOf(MQTT_SCHEME, MQTTS_SCHEME, HTTP_SCHEME, HTTPS_SCHEME)

        fun deserialize(input: String?): List<EndpointInfo>? {
            return if (input != null && input != "null")
                DataTypes.convertToList(input)
            else null
        }
    }
}
