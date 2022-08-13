package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

// it is a lightweight subscription representation, so ignore other fields from a complete subscription
@JsonIgnoreProperties(ignoreUnknown = true)
data class Subscription(
    val id: URI,
    val type: String = "Subscription",
    val name: String? = null,
    val description: String? = null,
    val createdAt: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
)
