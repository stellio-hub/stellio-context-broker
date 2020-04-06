package com.egm.stellio.shared.model

data class Subscription(
    val id: String,
    val type: String = "Subscription",
    val name: String? = null,
    val description: String? = null
)
