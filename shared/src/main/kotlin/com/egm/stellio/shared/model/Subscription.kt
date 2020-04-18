package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

// it is a lightweight subscription representation, so ignore other fields from a complete subscription
@JsonIgnoreProperties(ignoreUnknown = true)
data class Subscription(
    val id: String,
    val type: String = "Subscription",
    val name: String? = null,
    val description: String? = null
)
