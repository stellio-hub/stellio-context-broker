package com.egm.datahub.context.subscription.model

import java.net.URI

data class Endpoint(
    val uri: URI,
    val accept: AcceptType
) {
    enum class AcceptType(val accept: String) {
        JSON("application/json"),
        JSONLD("application/ld+json")
    }
}
