package com.egm.stellio.subscription.model

import com.egm.stellio.subscription.model.Endpoint.AcceptType.JSON
import com.fasterxml.jackson.annotation.JsonProperty
import java.net.URI

data class Endpoint(
    val uri: URI,
    val accept: AcceptType = JSON,
    val receiverInfo: List<EndpointInfo>? = null,
    val notifierInfo: List<EndpointInfo>? = null
) {

    enum class AcceptType(val accept: String) {
        @JsonProperty("application/json")
        JSON("application/json"),

        @JsonProperty("application/ld+json")
        JSONLD("application/ld+json")
    }
}
