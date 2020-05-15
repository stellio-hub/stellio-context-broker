package com.egm.stellio.entity.web

data class BatchOperationResult(
    val success: ArrayList<String>,
    val errors: ArrayList<BatchEntityError>
)

data class BatchEntityError(
    val entityId: String,
    val error: ArrayList<String>
)