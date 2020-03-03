package com.egm.datahub.context.registry.model

data class UpdateResult(
    val updated: List<String>,
    val notUpdated: List<NotUpdatedDetails>
)

data class NotUpdatedDetails(
    val attributeName: String,
    val reason: String
)
